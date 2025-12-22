import sqlite3
import pandas as pd
import shutil
import os
import time

# Configuracion de rutas (asumiendo ejecución desde el directorio 'panno')
DB_PATH = "./assets/pgx_kb.sqlite3"
DATA_DIR = "./panno/data/output_review/"

CSV_FILES = {
    "ClinAnn": "ClinAnn_Review.csv",
    "GuidelineMerge": "GuidelineMerge_Review.csv",
    "GuidelineRule": "GuidelineRule_Review.csv"
}

def backup_database():
    if not os.path.exists(DB_PATH):
        print(f"No se encontró la base de datos en {DB_PATH}")
        return False
    
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    backup_path = f"{DB_PATH}.backup_{timestamp}"
    
    try:
        shutil.copy(DB_PATH, backup_path)
        print(f"Respaldo creado exitosamente: {backup_path}")
        return True
    except Exception as e:
        print(f"Error creando respaldo: {e}")
        return False

def update_table(conn, table_name, csv_file):
    csv_path = os.path.join(DATA_DIR, csv_file)
    if not os.path.exists(csv_path):
        print(f"Archivo CSV no encontrado: {csv_path}")
        return False
        
    print(f"\nActualizando tabla '{table_name}' desde {csv_file}...")
    
    try:
        # Leer CSV
        df = pd.read_csv(csv_path)
        # Reemplazar NaN con string vacío para evitar errores NOT NULL en SQLite
        df.fillna("", inplace=True)
        print(f"   Leídas {len(df)} filas del CSV.")
        
        cursor = conn.cursor()
        
        # Verificar conteo anterior
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count_before = cursor.fetchone()[0]
        print(f"   Registros en DB antes: {count_before}")
        
        # Borrar datos viejos
        cursor.execute(f"DELETE FROM {table_name}")
        print("    Datos antiguos borrados.")
        
        # Insertar nuevos datos
        # Esto usa pandas to_sql, que es conveniente. 'append' porque acabamos de borrar.
        df.to_sql(table_name, conn, if_exists='append', index=False)
        
        # Verificar nuevo conteo
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count_after = cursor.fetchone()[0]
        print(f"     Registros en DB ahora: {count_after}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error actualizando {table_name}: {e}")
        return False

def main():
    print(" INICIANDO ACTUALIZACIÓN DE BASE DE DATOS")
    
    # 1. Backup
    if not backup_database():
        print(" Abortando actualización por fallo en respaldo.")
        return

    # 2. Conexión
    try:
        conn = sqlite3.connect(DB_PATH)
        # Habilitar claves foráneas por si acaso, aunque pandas lo maneja bastante crudo
        conn.execute("PRAGMA foreign_keys = ON") 
        
        success_clinann = update_table(conn, "ClinAnn", CSV_FILES["ClinAnn"])
        success_guide = update_table(conn, "GuidelineMerge", CSV_FILES["GuidelineMerge"])
        success_rule = update_table(conn, "GuidelineRule", CSV_FILES["GuidelineRule"])
        
        if success_clinann and success_guide and success_rule:
            conn.commit()
            print("\n✨ TRANSACCIÓN COMPLETADA EXITOSAMENTE. Los cambios se han guardado.")
        else:
            conn.rollback()
            print("\n HUBO ERRORES. Se hizo ROLLBACK, la base de datos no se modificó.")
            
        conn.close()
        
    except Exception as e:
        print(f" Error de conexión o transacción: {e}")

if __name__ == "__main__":
    main()
