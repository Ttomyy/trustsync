import pandas as pd
import os
from pymongo import MongoClient
from dotenv import load_dotenv
import great_expectations as gx

load_dotenv()

# conexión a mongoDB
client = MongoClient(os.getenv("MONGO_URI"))
db = client[os.getenv("MONGO_DB")]
collection = db[ "cobros_eventos"]

docs = list(collection.find({}, {"_id" : 0  }))
df = pd.DataFrame(docs)

print(f"Documentos cargados: {len(df)}")
print(f"Columnas: {df.columns.tolist()}")

# -------- Great Expectations --------
context = gx.get_context()

data_source = context.data_sources.add_pandas("mongo_cobros")
data_asset = data_source.add_dataframe_asset("cobros_eventos")
batch_definition = data_asset.add_batch_definition_whole_dataframe("batch_completo")
batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

suite = context.suites.add(gx.ExpectationSuite(name="trustsyn_suite"))

# Expectativa  - cobrador no nulo
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="cobrador")
    
)

# Expectativa  2  - estado evento solo valores validos
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="estado_evento",
        value_set=["COBRO", "BLOQUEADO", "MOVIMIENTO"]
    )
)

# Expectativa 3 - sesgo: al menos 20% diversidad de cobros en bloqueados
df_bloqueado = df[df["estado_evento"] == "BLOQUEADO"]
if len(df_bloqueado) > 0:
    diversidad = df_bloqueado["cobrador"].nunique() / len(df_bloqueado)
    print(f"\nDiversidad cobradores BLOQUEADOS: {diversidad:.2%}")
    if diversidad < 0.2:
        print("ALERTA SESGO: Mas del 80% del BLOQUEADOS tine mismo cobrador.")
    else:
        print("Diversidad de cobradores BLOQUEADOS ACEPTABLE.")    
        
# VAlidación de expectativas
validation_definition = context.validation_definitions.add(
    gx.ValidationDefinition(
        name = "trustsyn_validation",
        data = batch_definition,
        suite = suite,
    )
)

results = validation_definition.run(batch_parameters={"dataframe": df})
print(f"\nResultados de validación: {results.success}")
for result in results.results:
    status = "PASSED" if result.success else "FAILED"
    print(f"{status} {result.expectation_config.type} -> {result.success}")
    
    