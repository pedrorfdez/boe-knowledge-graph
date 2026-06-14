import os
import pytest
from pathlib import Path

_PROJECT_ROOT = Path(os.environ.get("LEGISLATION_ROOT", Path(__file__).parent.parent.parent.parent))
ADAPTERS_DIR = _PROJECT_ROOT / "adapters"
ONTOLOGY_DIR = _PROJECT_ROOT / "ontology"

BOE_SUMARIO_RESPONSE = {
    "data": {
        "sumario": {
            "diario": {
                "numero": "13",
                "fecha": "20240115",
                "seccion": [
                    {
                        "nombre": "I. Disposiciones generales",
                        "departamento": [
                            {
                                "nombre": "MINISTERIO DE TRABAJO",
                                "epigrafe": [
                                    {
                                        "item": [
                                            {
                                                "id": "BOE-A-2024-999",
                                                "titulo": "Ley de prueba",
                                            }
                                        ]
                                    }
                                ],
                            }
                        ],
                    }
                ],
            }
        }
    }
}

BOE_DOCUMENT_RESPONSE = {
    "data": {
        "documento": {
            "metadatos": {
                "identificador": "BOE-A-2024-999",
                "titulo": "Ley de prueba",
                "fecha_publicacion": "20240115",
                "departamento": "MINISTERIO DE TRABAJO",
                "rango": "Ley",
                "estatus_derogacion": "En vigor",
            },
            "analisis": {
                "referencias": {
                    "anteriores": [
                        {
                            "referencia": {
                                "texto": "modifica",
                                "id": "BOE-A-1980-1000",
                            }
                        }
                    ],
                    "posteriores": [],
                },
            },
            "texto": """<html><body>
<p class="parrafo_1">Preámbulo: Esta ley tiene por objeto regular el trabajo.</p>
<p class="articulo"><span class="titulo_articulo">Artículo 1.</span> Objeto.</p>
<p class="parrafo">La presente ley regula las condiciones de trabajo.</p>
<p class="articulo"><span class="titulo_articulo">Artículo 2.</span> Ámbito de aplicación.</p>
<p class="parrafo">Se aplica a todos los trabajadores por cuenta ajena.</p>
<p class="disposicion_adicional"><span>Disposición adicional primera.</span> Texto adicional.</p>
</body></html>""",
        }
    }
}


@pytest.fixture
def boe_sumario_response():
    return BOE_SUMARIO_RESPONSE


@pytest.fixture
def boe_document_response():
    return BOE_DOCUMENT_RESPONSE
