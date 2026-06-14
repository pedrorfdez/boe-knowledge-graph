import pytest
from pipeline.adapters.models import ParseConfig


SAMPLE_HTML = """<html><body>
<p class="parrafo_1">Preámbulo de la ley.</p>
<p class="articulo"><span class="titulo_articulo">Artículo 1.</span> Objeto.</p>
<p class="parrafo">Esta ley regula el trabajo a distancia.</p>
<p class="articulo"><span class="titulo_articulo">Artículo 2.</span> Ámbito.</p>
<p class="parrafo">Se aplica a todos los trabajadores.</p>
<p class="disposicion_adicional"><span>Disposición adicional primera.</span> Texto adicional aquí.</p>
<p class="disposicion_final"><span>Disposición final primera.</span> Entrada en vigor.</p>
</body></html>"""


@pytest.fixture
def boe_parse_config():
    return ParseConfig(
        type="html",
        article_selector="p.articulo",
        article_title_selector="span.titulo_articulo",
        preamble_selectors=["p.parrafo_1", "p.preambulo", "p.exposicion_motivos"],
        provision_selectors={
            "disposicion_adicional": "additional",
            "disposicion_transitoria": "transitional",
            "disposicion_derogatoria": "repealing",
            "disposicion_final": "final",
        },
        annex_selector="p.anexo",
    )


def test_parse_extracts_preamble(boe_parse_config):
    from pipeline.parsers.html_parser import parse_html
    result = parse_html(SAMPLE_HTML, boe_parse_config)
    assert result["preamble_text"] is not None
    assert "Preámbulo" in result["preamble_text"]


def test_parse_extracts_articles(boe_parse_config):
    from pipeline.parsers.html_parser import parse_html
    result = parse_html(SAMPLE_HTML, boe_parse_config)
    assert len(result["articles"]) == 2
    assert result["articles"][0]["article_num"] == "Artículo 1"
    assert "trabajo a distancia" in result["articles"][0]["text"]
    assert result["articles"][1]["article_num"] == "Artículo 2"


def test_parse_articles_have_required_fields(boe_parse_config):
    from pipeline.parsers.html_parser import parse_html
    result = parse_html(SAMPLE_HTML, boe_parse_config)
    for article in result["articles"]:
        assert "article_id" in article
        assert "article_num" in article
        assert "text" in article


def test_parse_extracts_provisions(boe_parse_config):
    from pipeline.parsers.html_parser import parse_html
    result = parse_html(SAMPLE_HTML, boe_parse_config)
    provisions = result["provisions"]
    assert len(provisions) == 2
    additional = [p for p in provisions if p["type"] == "additional"]
    assert len(additional) == 1
    final = [p for p in provisions if p["type"] == "final"]
    assert len(final) == 1


def test_parse_empty_html_returns_empty_structure(boe_parse_config):
    from pipeline.parsers.html_parser import parse_html
    result = parse_html("<html><body></body></html>", boe_parse_config)
    assert result["preamble_text"] is None
    assert result["articles"] == []
    assert result["provisions"] == []
    assert result["annexes"] == []
