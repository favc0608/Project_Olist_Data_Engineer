# Contrato de plantillas (generador <-> constructores de tiendas)

El generador (`generador/`, Python estándar) lee `datos/` y rellena las plantillas de `plantillas/<tienda>/`
con `string.Template` (sintaxis `$variable` o `${variable}`). El resultado va a `sitio/<tienda>/`.

Reglas de sintaxis:
- Un `$` literal se escribe `$$`. **Para "R$" usar la variable `$rs`** (vale `R$`), así no hay que escapar.
- Si una plantilla usa una variable que no existe, el generador falla. Eso es intencional: detecta erratas.
- Los bucles (tarjetas, filas, páginas) los hace el generador con los **fragmentos**: el constructor escribe una
  tarjeta y el generador la repite.
- Toda ruta se arma con `$raiz` (`""` en la portada, `"../"` dentro de `busca/`, `categoria/` y `produto/`).

## 1. Archivos que entrega cada tienda (`plantillas/<tienda>/`)

| Archivo | Tipo | Se usa en |
|---|---|---|
| `layout.html` | Página base: `<html>`, `<head>`, header, menú, footer | Todas las páginas |
| `portada.html` | Contenido | `index.html` |
| `busca.html` | Contenido | `busca/pagina-N.html` |
| `categoria.html` | Contenido | `categoria/<slug>.html` |
| `produto.html` | Contenido | `produto/<sku>.html` |
| `ofertas.html` | Contenido | `ofertas.html` |
| `frete.html` | Contenido | `frete.html` |
| `vendedores.html` | Contenido | `vendedores.html` |
| `404.html` | Contenido | `404.html` |
| `fragmentos/tarjeta.html` | Fragmento | Una tarjeta de producto (listados, ofertas, destaques, relacionados) |
| `fragmentos/categoria_item.html` | Fragmento | Un elemento del menú o del carrusel de categorías |
| `fragmentos/pagina_item.html` | Fragmento | Un número de la paginación |
| `fragmentos/fila_frete.html` | Fragmento | Una fila de la tabla de 27 UF |
| `fragmentos/fila_regiao.html` | Fragmento | Una fila de "Calcule o frete" (5 regiones) en la ficha |
| `fragmentos/fila_spec.html` | Fragmento | Una fila de la tabla técnica |
| `fragmentos/resena.html` | Fragmento | Un comentario de comprador |
| `fragmentos/fila_vendedor.html` | Fragmento | Una fila de vendedores.html |
| `fragmentos/otro_vendedor.html` | Fragmento | Solo `mercadoabierto`: fila de "Outros vendedores" en la ficha |
| `fragmentos/variante.html` | Fragmento | Solo `compree`: una opción de variante en la ficha |
| `css/styles.css` | Estilos | Se copia tal cual |

Las ilustraciones van en `plantillas/_comun/svg/<categoria>.svg`, un SVG por cada una de las 6 categorías. Las
dibujan los constructores y el generador las inserta como `$svg`. Pueden variar el color con `currentColor`.

## 2. Variables de `layout.html`

| Variable | Ejemplo | Nota |
|---|---|---|
| `$titulo` | `Toalha de Banho Gigante - Ridley` | Para `<title>` |
| `$descricao` | texto | `<meta name="description">` |
| `$raiz` | `../` | Prefijo de rutas |
| `$version` | `2026-11-10T05:00Z-3fa9c1` | `<meta name="g1-version" content="$version">` (obligatorio) |
| `$head_extra` | `<link rel="next" href="pagina-3.html">` | Debe ir dentro de `<head>` (paginación, JSON-LD) |
| `$aviso` | `<div class="g1-aviso">Site simulado…</div>` | Debe ir justo después de `<body>` |
| `$menu_categorias` | (fragmentos `categoria_item` ya unidos) | Menú del header |
| `$conteudo` | (la página de contenido ya rellenada) | Cuerpo |
| `$tienda` | `Ridley` | Nombre visible |
| `$rs` | `R$` | Símbolo de moneda |

## 3. Variables de cada página de contenido

| Página | Variables |
|---|---|
| `portada.html` | `$destaques` (8 tarjetas), `$categorias` (6 `categoria_item`), `$ofertas_destaque` (4 tarjetas), `$frete_gratis_acima` (`79,00`) |
| `busca.html` | `$tarjetas` (12 + 1 patrocinada), `$pagina`, `$paginas` (4), `$desde`, `$hasta`, `$total`, `$paginacao` (fragmentos `pagina_item`), `$url_anterior` y `$url_seguinte` (vacías en los extremos) |
| `categoria.html` | `$categoria_nome`, `$categoria_slug`, `$tarjetas`, `$total`, `$svg` |
| `produto.html` | Todas las variables de producto (sección 4) más `$specs` (filas), `$fretes` (5 `fila_regiao`), `$resenas` (3 `resena`), `$relacionados` (4 tarjetas), `$outros_vendedores` (filas o vacío), `$variantes` (opciones o vacío), `$categoria_nome`, `$url_categoria` |
| `ofertas.html` | `$tarjetas` (descuento de 20% o más), `$termina_em` (`03:42:10`) |
| `frete.html` | `$filas_frete` (27 `fila_frete`), `$frete_gratis_acima`, `$cambio_usd_brl` (`5,42`; solo lo muestra `selva`) |
| `vendedores.html` | `$filas_vendedores` |
| `404.html` | (sin variables extra) |

## 4. Variables de producto (`tarjeta.html` y `produto.html`)

Comunes a todas las tiendas:

| Variable | Ejemplo | Nota |
|---|---|---|
| `$url_produto` | `../produto/MAB-1029384756.html` | Ya incluye `$raiz` |
| `$sku_loja` | `MAB-1029384756` | Valor de `data-sku`: SKU **propio de la tienda**, no el de Olist |
| `$ean` | `7891234567895` | Valor de `data-ean` |
| `$data_extra` | ` data-sponsored="true"` o vacío | Se pega dentro de la etiqueta del contenedor |
| `$titulo_produto` | `Jogo de Lençol Casal…` | Puede traer espacios y saltos de línea a propósito (trampa T4): **no recortar** |
| `$marca` | `Casa Serena` | |
| `$categoria_nome` | `Cama, Mesa e Banho` | |
| `$svg` | `<svg …>` | Ilustración de la categoría |
| `$badge_patrocinado` | `<span class="…">Patrocinado</span>` o vacío | |
| `$classe_estoque` | ` out-of-stock` o vacío | Se agrega al `class` del contenedor |
| `$texto_estoque` | `Em estoque` / `Esgotado` / `Restam apenas 3` | |
| `$rating` | `4,6` | |
| `$avaliacoes` | `1.284` | |
| `$desconto` | `14` | Sin `%` |
| `$frete_gratis` | `Frete grátis` o vacío | |

Propias de cada tienda (el generador las calcula y formatea; la plantilla **solo las coloca**):

| Tienda | Variables |
|---|---|
| `mercadoabierto` | `$preco_inteiro` (`1.299`), `$preco_centavos` (`90`), `$preco_lista_inteiro`, `$preco_lista_centavos`, `$parcelas` (`em 10x R$ 129,99 sem juros`), `$full` (`⚡ FULL` o vacío), `$vendedor` |
| `selva` | `$asin`, `$usd_texto` (`US$23.99`), `$usd_inteiro` (`23`), `$usd_fracao` (`99`), `$usd_lista_texto`, `$brl_estimado` (`aprox. R$ 131,95`), `$plus` (`✓plus` o vacío) |
| `compree` | `$preco_html` (`<span class="currency">R$</span><span class="price-value">39,90</span>` o el rango), `$preco_original` (`R$ 49,90`), `$vendidos` (`1,4 mil vendidos`), `$localizacao` (`São Paulo - SP`) |
| `ridley` | `$partnumber` (`RDL-20049381`), `$preco_de` (`R$ 199`), `$preco_por` (`R$ 149`), `$preco_cartao` (`R$ 139`), `$parcelas` (`ou 6x de R$ 24,83`) |

### 4.1 Reglas de marcado de la ficha (`produto.html`)

- El **contenedor principal** de la ficha lleva `data-sku="$sku_loja"` y `data-ean="$ean"`, y es el **primer**
  elemento de la página con `data-sku`. Los relacionados van después.
- Dentro de ese contenedor, el precio usa **el mismo marcado y las mismas clases que la tarjeta** de esa tienda
  (sección 6 de AGENTS.md). Así el scraper verifica que el precio del listado sea igual al de la ficha (regla R12).
- `$specs` va dentro de un `<table>` (tabla técnica) y `$fretes` dentro de otro `<table class="g1-fretes">`.
- En `mercadoabierto`, `$outros_vendedores` va dentro de `<table class="ui-pdp-other-sellers">`.

## 5. Variables de los fragmentos chicos

| Fragmento | Variables |
|---|---|
| `categoria_item.html` | `$url`, `$nome`, `$svg`, `$total` |
| `pagina_item.html` | `$url`, `$numero`, `$classe_atual` (` is-current` o vacío) |
| `fila_frete.html` | `$uf`, `$estado`, `$regiao`, `$valor` (`R$ 18,90`), `$prazo` (`5 a 7 dias úteis`) |
| `fila_regiao.html` | `$regiao`, `$valor`, `$prazo` |
| `fila_spec.html` | `$nome`, `$valor` |
| `resena.html` | `$autor`, `$nota` (`5`), `$data` (`03/11/2026`), `$texto` |
| `fila_vendedor.html` / `otro_vendedor.html` | `$vendedor`, `$uf`, `$reputacao`, `$vendas`, `$preco` (solo en `otro_vendedor`) |
| `variante.html` | `$nome` (`Azul / Casal`), `$preco` (`R$ 59,90`) |

## 6. Cómo previsualizar

```
.venv\Scripts\python.exe competencia\generador\construir.py --fecha 2026-11-10
```
Genera `competencia/sitio/` y corre la prueba de contrato. Se puede abrir `sitio/index.html` en el navegador.
