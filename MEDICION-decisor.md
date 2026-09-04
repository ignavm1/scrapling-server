# Medicion en vivo — resolutor de decisor

Fecha: **2026-09-03**. Proxy configurado: **NO**.
Generado por `bench/medir_decisor.py`. No editar a mano: se reescribe
en cada corrida.

| Empresa | Dominio | Veredicto | Decisores | Fetches | Paginas | Crudos | Bloqueados | ms |
|---|---|---|---|---|---|---|---|---|
| Fintual | fintual.cl | decisor | 1 | 3 | 2 | 0 | — | 2799 |
| Buk | buk.cl | decisor | 2 | 12 | 3 | 94 | ddglite, duckduckgo | 5723 |
| Betterfly | betterfly.com | bloqueado | 0 | 12 | 3 | 55 | ddglite, duckduckgo, brave | 5524 |
| Toteat | toteat.com | bloqueado | 0 | 10 | 1 | 20 | duckduckgo, ddglite, brave | 3537 |

**Empresas con decisor encontrado: 2 de 4 (3 personas).**

## Fintual

- **Omar Larré** — Co-founder & CIO · score 1.0 · via sitio_directo/sitio · https://fintual.cl/equipo-inversiones/
  - cargo: Co-founder & CIO (1.00)
  - publicado en el sitio de la propia empresa
  - el texto nombra a la empresa buscada
  - leido del texto de la pagina, no del snippet

## Buk

- **Jaime Arrieta** — Founder · score 0.65 · via cargo_directo/brave · https://rio.websummit.com/attendees/rio25/4f811c27-773d-448c-a7e8-ed2af7da199b/jaime-arrieta-boetsch
  - cargo: Founder (1.00)
  - el texto nombra a la empresa buscada
  - tambien visto como "Jaime Arrieta Boetsch"
- **Ricardo Sateler** — Co-Founder · score 0.65 · via cargo_directo/brave · https://craft.co/buk-chile/executives
  - cargo: Co-Founder (1.00)
  - el texto nombra a la empresa buscada

## Corridas que no pudieron mirar

Un vacio por bloqueo **no** significa que la empresa no publique a su
decisor. Sin `PROXY_URL` los buscadores bloquean a los pocos requests
(F1) y queda solo Bing, ya clasificado como fuente hostil (F6).

- Betterfly: ddglite=captcha, duckduckgo=captcha, brave=captcha
- Toteat: duckduckgo=captcha, ddglite=captcha, brave=captcha
