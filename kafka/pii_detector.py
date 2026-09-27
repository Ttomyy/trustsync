import os
from pymongo import MongoClient
from dotenv import load_dotenv
from presidio_analyzer import AnalyzerEngine
from presidio_analyzer.nlp_engine import NlpEngineProvider
from presidio_anonymizer import AnonymizerEngine
from presidio_analyzer import PatternRecognizer,Pattern, EntityRecognizer
# -- Confirguración de Presidio Analyzer y Anonymizer

load_dotenv()


provider = NlpEngineProvider(nlp_configuration={
    "nlp_engine_name": "spacy",
    "models": [{"lang_code": "es", "model_name": "es_core_news_lg"}]
})

nlp_engine = provider.create_engine()

# CReconocimiento de NIF (Número de Identificación Fiscal) en España
nif_recognizer =    PatternRecognizer(supported_entity="NIF",
                                      supported_language="es",
                                      patterns=[Pattern(name="nif_pattern",
                                                        regex=r"\b\d{8}[A-Z]\b",
                                                        score=0.85
                                                        )
                                                ]
                                      )

telefono_recognizer = PatternRecognizer(
    supported_entity="PHONE_ES",
    supported_language="es",
    patterns=[
        Pattern(
            name="telefono_es",
            regex=r"\b[6-9]\d{8}\b",
            score=0.85
        )
    ]
)
# 4. Crear el analyzer CON los reconocedores custom
analyzer = AnalyzerEngine(nlp_engine=nlp_engine,supported_languages=["es", "en"])
analyzer.registry.add_recognizer(nif_recognizer)
analyzer.registry.add_recognizer(telefono_recognizer)

#analyzer = AnalyzerEngine(nlp_engine=provider.create_engine())
#analyzer = AnalyzerEngine()
anonymizer = AnonymizerEngine()

# -- Conectar a MongoDB Atlas
MONGO_URI = os.getenv('MONGO_URI')

client = MongoClient(MONGO_URI)
coleccion = client[os.getenv('MONGO_DB')]['cobros_eventos']
coleccion_pii = client[os.getenv('MONGO_DB')]['cobros_eventos_pii']
print("🔍 Iniciando el detector de PII en eventos de cobros...\n")

# --- Procesar eventos de cobros y detectar PII en comentarios ---------
documentos = coleccion.find(
    {"comentario": {"$exists": True, "$ne": ""}},
    {"_id": 1, "comentario": 1 }

)   

pii_encontrada = 0
sin_pii = 0

for doc in documentos:
    texto = doc.get("comentario", "")
    resultados = analyzer.analyze(text=texto, language='es')
    
    if resultados:
        pii_encontrada += 1
        
        # -- anonimizar el texto con PII detectado
        texto_anonimizado = anonymizer.anonymize(
            text=texto, 
            analyzer_results=resultados
            ).text
        
        tipos = list(set(r.entity_type for r in resultados  ))
        
        print(f"🛑 PII detectada en documento {doc['_id']}:")
        print(f" Original {texto[:80]}...")
        print(f" Anonimizado {texto_anonimizado[:80]}...")
    
        coleccion_pii.update_one(
            {"_id": doc["_id"]},
            {
                "$set": {
                    "comentario_original": texto,
                    "comentario_anonimizado": texto_anonimizado,
                    "tipos_pii_detectados": tipos
                }
            }
        )
    else:
        sin_pii += 1
        coleccion.update_one(
            {"_id": doc["_id"]},
            {"$set": {"pii_detectada": False,
                      "pii_tipos": []}}
            
        )
        print(f"✅ Sin PII en documento {doc['_id']}: {texto[:80]}...")
        
client.close()
print(f"\n🔍 Detección de PII completada. Documentos con PII: {pii_encontrada}")
      
print(f"Documentos sin PII: {sin_pii}")
        