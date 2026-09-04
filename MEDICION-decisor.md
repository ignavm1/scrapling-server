# Medicion en vivo — resolutor de decisor

Fecha: **2026-09-04**. Proxy configurado: **NO**.
Generado por `bench/medir_decisor.py`. No editar a mano: se reescribe
en cada corrida.

| Empresa | Dominio | Veredicto | Decisores | Fetches | Paginas | Crudos | Bloqueados | ms |
|---|---|---|---|---|---|---|---|---|
| Fintual | fintual.cl | decisor | 1 | 3 | 2 | 0 | — | 2770 |
| Buk | buk.cl | bloqueado | 0 | 12 | 3 | 0 | brave, duckduckgo | 8150 |
| Betterfly | betterfly.com | bloqueado | 0 | 12 | 3 | 0 | brave, duckduckgo | 8724 |
| Toteat | toteat.com | bloqueado | 0 | 10 | 1 | 0 | brave, duckduckgo | 6277 |

**Empresas con decisor encontrado: 1 de 4 (1 personas).**

## Fintual

- **Omar Larré** — Co-founder & CIO · score 1.0 · via sitio_directo/sitio · https://fintual.cl/equipo-inversiones/
  - cargo: Co-founder & CIO (1.00)
  - publicado en el sitio de la propia empresa
  - el texto nombra a la empresa buscada
  - leido del texto de la pagina, no del snippet

## Corridas que no pudieron mirar

Un vacio por bloqueo **no** significa que la empresa no publique a su
decisor. Sin `PROXY_URL` los buscadores bloquean a los pocos requests
(F1) y queda solo Bing, ya clasificado como fuente hostil (F6).

- Buk: brave=captcha, duckduckgo=status-403
- Betterfly: brave=captcha, duckduckgo=status-403
- Toteat: brave=captcha, duckduckgo=status-403
