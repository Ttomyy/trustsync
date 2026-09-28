import os
import google.generativeai as genai
from pymongo import MongoClient
from dotenv import load_dotenv
import json

load_dotenv()

# --- Configuración Gemini ---
genai.configure(api_key=os.getenv("API_KEY_GEM_TRUST"))

# --- Conexión MongoDB ---
client = MongoClient(os.getenv("MONGO_URI"))
db = client[os.getenv("MONGO_DB")]
collection = db["cobros_eventos"]

# --- Herramientas del agente ---

def query_mongo() -> dict:
    """Obtiene estadísticas de cobros_eventos en MongoDB."""
    total = collection.count_documents({})
    por_estado = {}
    for estado in ["COBRO", "BLOQUEADO", "MOVIMIENTO"]:
        por_estado[estado] = collection.count_documents({"estado_evento": estado})
    
    # Diversidad de cobradores en BLOQUEADO
    bloqueados = list(collection.find(
        {"estado_evento": "BLOQUEADO"}, {"cobrador": 1, "_id": 0}
    ))
    cobradores = [d["cobrador"] for d in bloqueados if "cobrador" in d]
    diversidad = len(set(cobradores)) / len(cobradores) if cobradores else 0

    return {
        "total_documentos": total,
        "por_estado": por_estado,
        "diversidad_cobradores_bloqueado": round(diversidad, 4),
        "alerta_sesgo": diversidad < 0.20
    }

def get_dbt_results() -> dict:
    """Lee el último resultado real de dbt test desde los artifacts."""
    import json
    
    run_results_path = (
        "/home/tomy/trustsync/proyectos/trustsync_dbt/target/run_results.json"
    )
    
    try:
        with open(run_results_path) as f:
            data = json.load(f)
        
        results = data.get("results", [])
        fallidos = [r for r in results if r.get("status") not in ("pass", "success")]
        
        return {
            "status": "pass" if not fallidos else "fail",
            "tests_ejecutados": len(results),
            "tests_fallidos": len(fallidos),
            "ultimo_run": data.get("metadata", {}).get("generated_at", "desconocido"),
            "elapsed_seconds": round(data.get("elapsed_time", 0), 2),
        }
    except FileNotFoundError:
        return {"status": "sin_datos", "error": "run_results.json no encontrado"}

def get_gx_alerts() -> dict:
    """Obtiene alertas de Great Expectations desde MongoDB."""
    bloqueados = list(collection.find(
        {"estado_evento": "BLOQUEADO"}, {"cobrador": 1, "_id": 0}
    ))
    cobradores = [d["cobrador"] for d in bloqueados if "cobrador" in d]
    diversidad = len(set(cobradores)) / len(cobradores) if cobradores else 0

    alertas = []
    if diversidad < 0.20:
        top_cobrador = max(set(cobradores), key=cobradores.count)
        pct = cobradores.count(top_cobrador) / len(cobradores)
        alertas.append(
            f"SESGO DETECTADO: {top_cobrador} representa el {pct:.1%} de BLOQUEADOS"
        )

    return {
        "alertas": alertas,
        "expectativas_ok": diversidad >= 0.20
    }

# --- Definición de herramientas para Gemini ---
tools = [
    {
        "function_declarations": [
            {
                "name": "query_mongo",
                "description": "Obtiene estadísticas actuales de cobros_eventos en MongoDB: total de documentos, distribución por estado y diversidad de cobradores en eventos BLOQUEADO.",
            },
            {
                "name": "get_dbt_results",
                "description": "Obtiene el resultado del último dbt test: cuántos tests se ejecutaron y si hubo fallos.",
            },
            {
                "name": "get_gx_alerts",
                "description": "Obtiene las alertas de Great Expectations: si hay sesgo de cobradores u otras anomalías detectadas.",
            },
        ]
    }
]

tool_map = {
    "query_mongo": query_mongo,
    "get_dbt_results": get_dbt_results,
    "get_gx_alerts": get_gx_alerts,
}

# --- Agente ---
model = genai.GenerativeModel(
    model_name="gemini-3.8-flash",
    tools=tools,
)

pregunta = "¿Cuál es el estado actual del pipeline TrustSync? ¿Hay anomalías o alertas que deba conocer?"

print(f"Pregunta: {pregunta}\n")
print("=" * 60)

chat = model.start_chat()
response = chat.send_message(pregunta)

# Bucle agente: ejecuta herramientas hasta que Gemini responda en texto
while True:
    # ¿Gemini quiere usar una herramienta?
    tool_calls = [
        part for part in response.candidates[0].content.parts
        if hasattr(part, "function_call") and part.function_call.name
    ]

    if not tool_calls:
        # Gemini respondió en texto — fin del bucle
        print("\nRespuesta del agente:")
        print(response.text)
        break

    # Ejecutar cada herramienta que pidió Gemini
    tool_results = []
    for part in tool_calls:
        fn_name = part.function_call.name
        print(f"→ Agente llama: {fn_name}()")
        result = tool_map[fn_name]()
        print(f"  Resultado: {json.dumps(result, ensure_ascii=False)}")

        tool_results.append(
            genai.protos.Part(
                function_response=genai.protos.FunctionResponse(
                    name=fn_name,
                    response={"result": result},
                )
            )
        )

    # Devolver resultados a Gemini
    response = chat.send_message(tool_results)