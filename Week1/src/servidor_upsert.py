from fastapi import FastAPI, Body
import pandas as pd
from sqlalchemy import create_engine, text 
import uvicorn

app = FastAPI()
engine = create_engine('sqlite:///mi_base_de_datos_olist.db')

@app.post("/cargar_lote")
async def recibir_datos(datos: list = Body(...)):
    try:
        df_nuevos = pd.DataFrame(datos)
        ids_a_cargar = tuple(df_nuevos['order_id'].tolist())
        
        with engine.begin() as conn:
            # Envolvemos el string en text() para que SQLAlchemy lo reconozca como ejecutable
            if len(ids_a_cargar) == 1:
                stmt = text(f"DELETE FROM ventas_reportadas WHERE order_id = '{ids_a_cargar[0]}'")
            else:
                stmt = text(f"DELETE FROM ventas_reportadas WHERE order_id IN {ids_a_cargar}")
            
            conn.execute(stmt)
            
            # El to_sql sigue igual porque Pandas maneja su propia lógica interna
            df_nuevos.to_sql('ventas_reportadas', con=conn, if_exists='append', index=False)
            
        return {"status": "Éxito", "Upsert_realizado": len(df_nuevos)}
    
    except Exception as e:
        return {"status": "Error", "detalle": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)