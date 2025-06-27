import os
import pandas as pd
import shutil
import logging
from google.cloud import bigquery
from google.oauth2 import service_account

# Configuración del logging para ver el progreso y los errores.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BasketballETL:
    """
    ETL para procesar datos de baloncesto de Kaggle y cargarlos a BigQuery.
    Esta versión está adaptada para procesar y cargar ÚNICAMENTE la tabla 'player'.
    """
    def __init__(self, project_id: str, dataset_id: str, credentials_path: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.credentials_path = credentials_path
        self.data_directory = './data'
        self.csv_directory = self.data_directory
        try:
            credentials = service_account.Credentials.from_service_account_file(self.credentials_path)
            self.client = bigquery.Client(credentials=credentials, project=self.project_id)
            logger.info("✅ Cliente de BigQuery inicializado correctamente.")
        except Exception as e:
            logger.error(f"❌ Error al inicializar el cliente de BigQuery. Verifica la ruta de las credenciales: {e}")
            raise

    def download_dataset(self, dataset_name: str = 'wyattowalsh/basketball'):
        """Descarga y descomprime el dataset de Kaggle."""
        try:
            logger.info(f"📥 Descargando dataset '{dataset_name}' de Kaggle...")
            if os.path.exists(self.data_directory):
                shutil.rmtree(self.data_directory)
            os.makedirs(self.data_directory, exist_ok=True)

            os.system(f"kaggle datasets download -d {dataset_name} -p {self.data_directory} --unzip")
            logger.info("✅ Descarga y descompresión completada.")

            found_path = False
            for root, dirs, files in os.walk(self.data_directory):
                if any(f.endswith('.csv') for f in files):
                    self.csv_directory = root
                    logger.info(f"📁 Directorio de CSVs encontrado en: '{self.csv_directory}'")
                    found_path = True
                    break
            
            if not found_path:
                logger.error("❌ No se encontraron archivos CSV en la estructura del dataset descargado.")
                raise FileNotFoundError("No se encontraron CSVs luego de la descarga.")

        except Exception as e:
            logger.error(f"❌ Error en la descarga de Kaggle. Asegúrate de tener 'kaggle.json' configurado: {e}")
            raise

    def validate_columns(self, df: pd.DataFrame, required_cols: list, file_path: str) -> bool:
        """Valida que las columnas requeridas existan en el DataFrame."""
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            logger.error(f"❌ Columnas faltantes en '{os.path.basename(file_path)}': {missing}. Columnas disponibles: {list(df.columns)}")
            return False
        return True

    def load_to_bigquery(self, df: pd.DataFrame, table_name: str):
        """Carga un DataFrame a la tabla especificada en BigQuery."""
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
        try:
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True) 
            job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result() 
            logger.info(f"🗄️  Datos cargados exitosamente en la tabla '{table_id}'")
        except Exception as e:
            logger.error(f"❌ Error al cargar datos en BigQuery para la tabla '{table_id}': {e}")

    def _process_csv(self, file_path: str, required_cols: list, rename_map: dict = None, transformations=None) -> pd.DataFrame:
        """Función auxiliar genérica para procesar un archivo CSV."""
        try:
            df = pd.read_csv(file_path, low_memory=False)
            df.columns = [col.upper() for col in df.columns] 
            
            if not self.validate_columns(df, required_cols, file_path): 
                return pd.DataFrame()

            if rename_map:
                df.rename(columns=rename_map, inplace=True)
            
            if transformations:
                df = transformations(df)

            return df
        except Exception as e:
            logger.error(f"❌ Error al procesar el archivo '{os.path.basename(file_path)}': {e}")
            return pd.DataFrame()

    def process_player(self, file_path: str) -> pd.DataFrame:
        """Procesa el archivo de jugadores (player.csv)."""
        return self._process_csv(file_path, ['ID'], {'ID': 'player_id'})

    def get_load_order(self) -> list:
        """
        Define el orden de procesamiento.
        Solo se incluye la tabla 'player'.
        """
        return [
            {'csv': 'player.csv', 'func': self.process_player, 'table': 'player'},
        ]

def main():
    # --- LÍNEA AÑADIDA ---
    logger.info("🚀 Iniciando la actualización de datos...")
    
    # --- CONFIGURACIÓN DEL USUARIO ---
    PROJECT_ID = "nba-dapf"
    DATASET_ID = "team_history"
    CREDENTIALS_PATH = "C:\\Users\\Lautaro\\Keys\\nba-dapf-c0ebff4fd7aa.json"

    etl = None # Inicializamos etl como None para que esté disponible en el bloque finally
    try:
        etl = BasketballETL(PROJECT_ID, DATASET_ID, CREDENTIALS_PATH)
        etl.download_dataset()
        
        load_order = etl.get_load_order()

        for item in load_order:
            file_path = os.path.join(etl.csv_directory, item['csv'])
            if not os.path.exists(file_path):
                logger.warning(f"⚠️  Archivo no encontrado, se omite: {file_path}")
                continue
            
            logger.info(f"🔄  Procesando archivo '{item['csv']}' para la tabla '{item['table']}'...")
            df = item['func'](file_path)
            
            if df.empty:
                logger.warning(f"-> DataFrame vacío para '{item['table']}' después del procesamiento, se omite carga.")
            else:
                logger.info(f"-> ✅  Procesado exitoso. {len(df)} registros listos para cargar.")
                etl.load_to_bigquery(df, item['table'])

    except Exception as e:
        logger.critical(f"💥 Fallo crítico en la ejecución del ETL: {e}", exc_info=True)

    finally:
        # --- SECCIÓN DE LIMPIEZA ---
        # Este bloque se ejecutará siempre, al final del proceso.
        if etl and os.path.exists(etl.data_directory):
            logger.info(f"🧹 Limpiando directorio de datos temporales ('{etl.data_directory}')...")
            shutil.rmtree(etl.data_directory)
            logger.info("✅ Directorio temporal eliminado.")

if __name__ == '__main__':
    main()