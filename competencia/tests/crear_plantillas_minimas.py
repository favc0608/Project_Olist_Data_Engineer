"""Crea plantillas mínimas (sin diseño) que cumplen el contrato, para probar el generador y los scrapers
sin depender de las plantillas finales. Salida: tests/fixtures/plantillas_minimas/.
"""
from pathlib import Path

DESTINO = Path(__file__).resolve().parent / "fixtures" / "plantillas_minimas"

LAYOUT = """<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="robots" content="noindex, nofollow">
<meta name="g1-version" content="$version">
<title>$titulo</title>
<meta name="description" content="$descricao">
<link rel="stylesheet" href="${raiz}css/styles.css">
$head_extra
</head>
<body>
$aviso
<header><a href="${raiz}index.html">$tienda</a> <nav>$menu_categorias</nav>
<a href="${raiz}busca/pagina-1.html">Busca</a> <a href="${raiz}ofertas.html">Ofertas</a>
<a href="${raiz}frete.html">Frete</a> <a href="${raiz}vendedores.html">Vendedores</a></header>
<main>$conteudo</main>
</body>
</html>
"""

TARJETAS = {
    "mercadoabierto": """<li class="ui-search-layout__item$classe_estoque" data-sku="$sku_loja" data-ean="$ean"$data_extra>
$badge_patrocinado $svg
<h2 class="ui-search-item__title"><a href="$url_produto" class="ui-search-link">$titulo_produto</a></h2>
<span class="ui-search-item__brand-discoverability">$marca</span>
<s class="andes-money-amount andes-money-amount--previous"><span class="andes-money-amount__currency-symbol">$rs</span><span class="andes-money-amount__fraction">$preco_lista_inteiro</span><span class="andes-money-amount__cents">$preco_lista_centavos</span></s>
<div class="ui-search-price__second-line"><span class="andes-money-amount__currency-symbol">$rs</span><span class="andes-money-amount__fraction">$preco_inteiro</span><span class="andes-money-amount__cents">$preco_centavos</span></div>
<span class="ui-search-price__discount">$desconto% OFF</span>
<span class="ui-search-installments">$parcelas</span>
<span class="ui-search-item__fulfillment">$full $frete_gratis</span>
<span class="ui-search-item__stock-status">$texto_estoque</span>
<span class="ui-search-reviews__rating-number">$rating</span> <span class="ui-search-reviews__amount">($avaliacoes)</span>
<span class="ui-search-item__seller">$vendedor</span>
</li>""",
    "selva": """<div class="s-result-item$classe_estoque" data-component-type="s-search-result" data-asin="$asin" data-sku="$sku_loja" data-ean="$ean"$data_extra>
$badge_patrocinado $svg
<h2><a class="a-link-normal" href="$url_produto"><span class="a-size-medium a-text-normal">$titulo_produto</span></a></h2>
<span class="a-price"><span class="a-offscreen">$usd_texto</span><span aria-hidden="true"><span class="a-price-symbol">US$$</span><span class="a-price-whole">$usd_inteiro<span class="a-price-decimal">.</span></span><span class="a-price-fraction">$usd_fracao</span></span></span>
<span class="a-price a-text-price" data-a-strike="true"><span class="a-offscreen">$usd_lista_texto</span></span>
<span class="fx-estimate">$brl_estimado</span>
<span class="a-icon-alt">$rating de 5 estrelas</span> <span class="s-underline-text">$avaliacoes</span>
<span class="s-stock-status">$texto_estoque</span> <span class="s-prime-badge">$plus</span>
</div>""",
    "compree": """<div class="shopee-search-item-result__item$classe_estoque" data-sqe="item" data-sku="$sku_loja" data-ean="$ean"$data_extra>
$badge_patrocinado $svg
<div class="shopee-item-card__text-name"><a href="$url_produto">$titulo_produto</a></div>
<span class="shopee-item-card__original-price">$preco_original</span>
<div class="shopee-item-card__current-price">$preco_html</div>
<div class="shopee-item-card__discount-badge">-$desconto%</div>
<div class="shopee-item-card__sold-count">$vendidos</div>
<span class="shopee-item-card__rating">★ $rating</span>
<span class="shopee-item-card__location">$localizacao</span>
<span class="shopee-item-card__stock-badge">$texto_estoque</span>
</div>""",
    "ridley": """<article class="catalog-product-item$classe_estoque" data-partnumber="$partnumber" data-sku="$sku_loja" data-ean="$ean"$data_extra>
$badge_patrocinado $svg
<div class="brand-logo"><span>$marca</span></div>
<h3 class="catalog-product-details__name"><a href="$url_produto">$titulo_produto</a></h3>
<div class="catalog-prices">
<div class="catalog-prices__list-price"><span class="price-label">De:</span> <span class="price-value">$preco_de</span></div>
<div class="catalog-prices__offer-price"><span class="price-label">Por:</span> <span class="price-value">$preco_por</span></div>
<div class="catalog-prices__card-price"><span class="price-label">No cartão Ridley:</span> <span class="price-value">$preco_cartao</span></div>
<div class="catalog-prices__installments">$parcelas</div>
</div>
<span class="catalog-product-details__discount-tag">-$desconto%</span>
<div class="catalog-product-details__availability">$texto_estoque</div>
</article>""",
}

PAGINAS = {
    "portada": "<h1>$tienda</h1><section>$categorias</section><section>$destaques</section><section>$ofertas_destaque</section><p>Frete grátis acima de $rs $frete_gratis_acima</p>",
    "busca": "<h1>Mostrando $desde-$hasta de $total resultados</h1><ul>$tarjetas</ul><nav><a href=\"$url_anterior\">Anterior</a> $paginacao <a href=\"$url_seguinte\">Próxima</a></nav>",
    "categoria": "<h1>$categoria_nome ($total)</h1>$svg<ul>$tarjetas</ul>",
    "ofertas": "<h1>Ofertas</h1><p>Termina em $termina_em</p><ul>$tarjetas</ul>",
    "frete": "<h1>Frete</h1><p>Câmbio $cambio_usd_brl</p><table>$filas_frete</table>",
    "vendedores": "<h1>Vendedores</h1><table>$filas_vendedores</table>",
    "404": "<h1>Página não encontrada</h1>",
}
FRAGMENTOS = {
    "categoria_item": '<a href="$url">$nome ($total)</a>',
    "pagina_item": '<a class="page$classe_atual" href="$url">$numero</a>',
    "fila_frete": "<tr><td>$uf</td><td>$estado</td><td>$regiao</td><td>$valor</td><td>$prazo</td></tr>",
    "fila_regiao": "<tr><td>$regiao</td><td>$valor</td><td>$prazo</td></tr>",
    "fila_spec": "<tr><th>$nome</th><td>$valor</td></tr>",
    "resena": "<blockquote>$autor $nota $data $texto</blockquote>",
    "fila_vendedor": "<tr><td>$vendedor</td><td>$uf</td><td>$reputacao</td><td>$vendas</td></tr>",
    "otro_vendedor": '<tr class="ui-pdp-other-sellers__row"><td>$vendedor</td><td>$preco</td></tr>',
    "variante": '<span class="shopee-option-pill">$nome $preco</span>',
}


def main():
    for tienda, tarjeta in TARJETAS.items():
        base = DESTINO / tienda
        (base / "fragmentos").mkdir(parents=True, exist_ok=True)
        (base / "css").mkdir(exist_ok=True)
        (base / "layout.html").write_text(LAYOUT, encoding="utf-8")
        (base / "css" / "styles.css").write_text("body{font-family:Arial,sans-serif}\n", encoding="utf-8")
        for nombre, html in PAGINAS.items():
            (base / f"{nombre}.html").write_text(html, encoding="utf-8")
        ficha = tarjeta.replace("$url_produto", "$url_categoria") + (
            "<table>$specs</table><table class=\"g1-fretes\">$fretes</table>$resenas<table class=\"ui-pdp-other-sellers\">$outros_vendedores</table>"
            "$variantes<h2>Relacionados</h2>$relacionados")
        (base / "produto.html").write_text(ficha, encoding="utf-8")
        (base / "fragmentos" / "tarjeta.html").write_text(tarjeta, encoding="utf-8")
        for nombre, html in FRAGMENTOS.items():
            (base / "fragmentos" / f"{nombre}.html").write_text(html, encoding="utf-8")
    print(f"Plantillas mínimas en {DESTINO}")


if __name__ == "__main__":
    main()
