from fastapi import FastAPI, Body # Importamos Body
import pandas as pd
from sqlalchemy import create_engine
import uvicorn

app = FastAPI()
engine = create_engine('sqlite:///mi_base_de_datos_olist.db')

@app.post("/cargar_lote")

async def recibir_datos(datos: list = Body(...)): 
    try:
        df_recibido = pd.DataFrame(datos)
        df_recibido.to_sql('ventas_reportadas', con=engine, if_exists='append', index=False) # if_exists='replace' sobreescribe
        return {"status": "Éxito", "filas": len(df_recibido)}
    except Exception as e:
        return {"status": "Error", "detalle": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)