# Medicion en vivo — resolutor de decisor

Fecha: **2026-09-03**. Proxy configurado: **NO**.
Generado por `bench/medir_decisor.py`. No editar a mano: se reescribe
en cada corrida.

| Empresa | Dominio | Veredicto | Decisores | Fetches | Paginas | Crudos | Bloqueados | ms |
|---|---|---|---|---|---|---|---|---|
| Fintual | fintual.cl | decisor | 1 | 3 | 2 | 0 | — | 2797 |
| Buk | buk.cl | decisor | 2 | 12 | 3 | 90 | duckduckgo, ddglite | 5481 |
| Betterfly | betterfly.com | decisor | 2 | 12 | 3 | 90 | ddglite, duckduckgo | 5501 |
| Toteat | toteat.com | bloqueado | 0 | 10 | 1 | 20 | brave, duckduckgo, ddglite | 4374 |

**Empresas con decisor encontrado: 3 de 4 (5 personas).**

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

## Betterfly

- **Cristobal Della Maggiora** — Co-Founder · score 0.65 · via cargo_directo/brave · https://craft.co/betterfly-209/executives
  - cargo: Co-Founder (1.00)
  - el texto nombra a la empresa buscada
- **Eduardo Dillamajora** — founder · score 0.65 · via cargo_directo/brave · https://qedinvestors.com/blog/podcast-betterfly-ceo-eduardo-della-maggiora-discusses-his-journey-to-disrupting-the-delivery-of-both-physical-and-financial-wellbeing-at-scale
  - cargo: founder (1.00)
  - el texto nombra a la empresa buscada

## Corridas que no pudieron mirar

Un vacio por bloqueo **no** significa que la empresa no publique a su
decisor. Sin `PROXY_URL` los buscadores bloquean a los pocos requests
(F1) y queda solo Bing, ya clasificado como fuente hostil (F6).

- Toteat: brave=captcha, duckduckgo=status-403, ddglite=status-403
