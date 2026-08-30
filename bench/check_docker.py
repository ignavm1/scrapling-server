#!/usr/bin/env python3
"""G14 -- la imagen Docker construye y sirve /health con el codigo nuevo.

Se prueba de verdad: build + run + peticion HTTP. Un Dockerfile que "se ve
bien" pero no copia el paquete nuevo produce un contenedor que arranca y falla
al primer import, y eso se descubre en el deploy.

Si Docker no esta disponible, el chequeo NO miente: valida estaticamente que el
Dockerfile copie todo lo necesario y lo dice claramente.
"""
from __future__ import annotations
import json
import pathlib
import shutil
import subprocess
import sys
import time
import urllib.request

RAIZ = pathlib.Path(__file__).resolve().parent.parent
TAG = "venara-discovery-check"
PUERTO = 8791


def validar_dockerfile() -> list[str]:
    df = (RAIZ / "Dockerfile").read_text(encoding="utf-8")
    fallas = []
    # El paquete nuevo TIENE que viajar en la imagen. Sin esta linea el
    # contenedor construye igual y muere al primer import.
    if "venara_discovery" not in df:
        fallas.append("el Dockerfile no copia el paquete venara_discovery/")
    if "scrapling_server.py" not in df:
        fallas.append("el Dockerfile no copia el entrypoint")
    if "requirements.txt" not in df:
        fallas.append("el Dockerfile no instala requirements.txt")
    if "USER " not in df:
        fallas.append("el contenedor correria como root")
    return fallas


def arrancar_entrypoint() -> bool:
    """Corre `python scrapling_server.py` -- el CMD literal del Dockerfile."""
    import os
    env = dict(os.environ, PORT=str(PUERTO))
    proc = subprocess.Popen([str(RAIZ / ".venv" / "bin" / "python"), "scrapling_server.py"],
                            cwd=RAIZ, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True, env=env)
    try:
        for _ in range(25):
            if proc.poll() is not None:
                print((proc.stdout.read() or "")[-1200:])
                return False
            try:
                with urllib.request.urlopen("http://127.0.0.1:8765/health", timeout=2) as r:
                    d = json.loads(r.read().decode())
                    print("entrypoint OK: /health -> %s" % d)
                    return d.get("status") == "ok"
            except Exception:
                time.sleep(1)
        return False
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()


def main() -> None:
    fallas = validar_dockerfile()
    if fallas:
        print("DOCKER FALLA:")
        for f in fallas:
            print("  - " + f)
        sys.exit(1)

    if not shutil.which("docker"):
        # Docker no esta disponible. NO se declara verificado el build de la
        # imagen: se verifica lo que si es demostrable aca -- que el MISMO
        # entrypoint que ejecuta el contenedor (`python scrapling_server.py`)
        # arranca y sirve /health. Eso descarta el fallo mas comun (un import
        # roto tras partir el modulo) sin mentir sobre lo que no se probo.
        print("docker no disponible: se valida el Dockerfile y se arranca el entrypoint real")
        if not arrancar_entrypoint():
            sys.exit("el entrypoint no sirvio /health")
        print("DOCKER VERIFICADO (Dockerfile estatico + entrypoint ejecutado; "
              "el build de la imagen NO se ejecuto: falta docker en esta maquina)")
        return

    print("construyendo la imagen...")
    b = subprocess.run(["docker", "build", "-t", TAG, "."], cwd=RAIZ,
                       capture_output=True, text=True)
    if b.returncode != 0:
        print(b.stdout[-1500:] + b.stderr[-1500:])
        sys.exit("el build de Docker fallo")

    cid = subprocess.run(
        ["docker", "run", "-d", "--rm", "-p", "%d:8765" % PUERTO, TAG],
        cwd=RAIZ, capture_output=True, text=True).stdout.strip()
    if not cid:
        sys.exit("no se pudo levantar el contenedor")
    try:
        salud = None
        for _ in range(30):
            try:
                with urllib.request.urlopen("http://127.0.0.1:%d/health" % PUERTO, timeout=2) as r:
                    salud = json.loads(r.read().decode())
                    break
            except Exception:
                time.sleep(1)
        if not salud:
            logs = subprocess.run(["docker", "logs", cid], capture_output=True, text=True)
            print((logs.stdout + logs.stderr)[-1500:])
            sys.exit("el contenedor no respondio /health")
        if salud.get("status") != "ok":
            sys.exit("/health devolvio %s" % salud)
        print("contenedor OK: /health -> %s" % salud)
    finally:
        subprocess.run(["docker", "stop", cid], capture_output=True)
    print("DOCKER VERIFICADO")


if __name__ == "__main__":
    main()
