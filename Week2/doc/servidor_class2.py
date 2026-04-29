from fastapi import FastAPI, Body
import pandas as pd
from sqlalchemy import create_engine, text, inspect
import uvicorn
import os

app = FastAPI()

ruta_actual = os.getcwd()
ruta_db = os.path.join(ruta_actual, 'external.db')
engine = create_engine(f'sqlite:///{ruta_db}', connect_args={"check_same_thread": False})

@app.post("/cargar_lote")
async def recibir_datos(datos: list = Body(...)):
    try:
        if not datos:
            return {"status": "Error", "detalle": "El lote de datos está vacío."}
            
        df_nuevos = pd.DataFrame(datos)
        
        ids_a_cargar = tuple(df_nuevos['ID'].tolist())
        
        with engine.begin() as conn:
            inspector = inspect(engine)
            
            if inspector.has_table("catalogo_externo"):
                if len(ids_a_cargar) == 1:
                    stmt = text(f"DELETE FROM catalogo_externo WHERE ID = '{ids_a_cargar[0]}'")
                else:
                    stmt = text(f"DELETE FROM catalogo_externo WHERE ID IN {ids_a_cargar}")
                
                conn.execute(stmt)
            
            df_nuevos.to_sql('catalogo_externo', con=conn, if_exists='append', index=False)
            
        return {"status": "Éxito", "Upsert_realizado": len(df_nuevos)}
    
    except Exception as e:
        return {"status": "Error", "detalle": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)