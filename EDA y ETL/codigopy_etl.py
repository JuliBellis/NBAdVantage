import pandas as pd
import numpy as np  # Se importa numpy para manejar tipos numéricos de forma robusta
import os

# --- CONFIGURACIÓN ---
# La 'r' antes de las comillas le indica a Python que lea la ruta
# tal cual, ignorando caracteres especiales. Es ideal para rutas en Windows.

# Cambia esta ruta por la ubicación REAL de tu archivo.
# Ejemplo de Windows: r"C:\Users\TuUsuario\Downloads\game.csv"

# Leer la ruta del archivo desde una variable de entorno o archivo de configuración
ruta_completa_original = os.getenv('RUTA_ARCHIVO_ORIGINAL', r"C:\Users\Ing. Pedro Rubio\Downloads\game.csv") # aca sustituir por la ruta de tu pc

# El nombre del nuevo archivo que se creará en la misma carpeta.
# Puedes cambiarle el nombre si lo deseas.
nombre_archivo_limpio = 'game_limpio_y_completo.csv'
# --------------------


# --- NO MODIFICAR DEBAJO DE ESTA LÍNEA ---

# Paso 0: Preparar las rutas de los archivos
# ------------------------------------------
# A partir de la ruta completa, el código extrae automáticamente la carpeta
# y crea la ruta completa para el nuevo archivo limpio.
ruta_carpeta = os.path.dirname(ruta_completa_original)
ruta_completa_limpia = os.path.join(ruta_carpeta, nombre_archivo_limpio)

print("--- INICIO DEL PROCESO DE LIMPIEZA ---")
print(f"Intentando leer el archivo desde: {ruta_completa_original}")

try:
    # Paso 1: Cargar el archivo CSV
    # -----------------------------
    # Se utiliza pandas para leer el archivo CSV y cargarlo en un DataFrame,
    # que es como una tabla de Excel pero en Python.
    df = pd.read_csv(ruta_completa_original)
    print(f"\n[PASO 1 - ÉXITO] Archivo '{os.path.basename(ruta_completa_original)}' cargado.")
    print(f"El archivo original tiene {len(df)} filas.")

    # Paso 2: Eliminar filas completamente duplicadas
    # -----------------------------------------------
    # df.drop_duplicates() busca y elimina todas las filas que son idénticas
    # a otra fila en todos sus valores.
    df_sin_duplicados = df.drop_duplicates()
    filas_eliminadas = len(df) - len(df_sin_duplicados)
    
    if filas_eliminadas > 0:
        print(f"\n[PASO 2 - COMPLETADO] Se eliminaron {filas_eliminadas} filas duplicadas.")
    else:
        print("\n[PASO 2 - COMPLETADO] No se encontraron filas duplicadas.")
    print(f"El archivo ahora tiene {len(df_sin_duplicados)} filas.")

    # Paso 3: Limpiar y rellenar valores nulos o en blanco
    # ----------------------------------------------------
    # Este es el paso más importante. Recorremos cada columna del DataFrame
    # para tratar los valores faltantes de forma inteligente.
    print("\n[PASO 3 - EN PROCESO] Limpiando valores nulos y en blanco...")
    
    # Creamos una copia para evitar advertencias de Python al modificar los datos.
    df_limpio = df_sin_duplicados.copy()

    for columna in df_limpio.columns:
        # Primero, verificamos si la columna es de tipo numérico (int, float, etc.).
        if pd.api.types.is_numeric_dtype(df_limpio[columna]):
            # Si es numérica, calculamos la media (promedio).
            media = df_limpio[columna].mean()
            # Contamos cuántos valores nulos hay antes de rellenar.
            nulos_antes = df_limpio[columna].isnull().sum()
            if nulos_antes > 0:
                # Rellenamos los valores nulos (NaN) con la media calculada.
                df_limpio[columna].fillna(media, inplace=True)
                print(f"  - Columna numérica '{columna}': Se rellenaron {nulos_antes} valores nulos con la media ({media:.2f}).")
        
        # Si la columna no es numérica (es de tipo 'object', es decir, texto).
        else:
            # Reemplazamos las celdas que solo contienen espacios en blanco por un valor nulo (NaN).
            # Esto unifica todos los tipos de "vacíos" para que sean más fáciles de tratar.
            df_limpio[columna].replace(r'^\s*$', np.nan, regex=True, inplace=True)
            
            # Contamos los nulos (originales + los que acabamos de crear a partir de espacios en blanco).
            nulos_antes = df_limpio[columna].isnull().sum()
            if nulos_antes > 0:
                # Como es una columna de texto, no podemos calcular una media.
                # La rellenamos con un texto descriptivo.
                df_limpio[columna].fillna('No Registrado', inplace=True)
                print(f"  - Columna de texto '{columna}': Se rellenaron {nulos_antes} valores vacíos/nulos con 'No Registrado'.")

    print("[PASO 3 - COMPLETADO] Limpieza de valores finalizada.")

    # Paso 4: Guardar el DataFrame final y limpio
    # ---------------------------------------------
    # El DataFrame 'df_limpio' ahora contiene los datos sin duplicados y sin valores nulos.
    # .to_csv() lo exporta a un nuevo archivo CSV.
    # index=False evita que pandas añada una columna extra con el número de fila.
    df_limpio.to_csv(ruta_completa_limpia, index=False, encoding='utf-8')
    print(f"\n[PASO 4 - ÉXITO] Archivo limpio guardado en: '{ruta_completa_limpia}'")
    print("\n--- PROCESO FINALIZADO ---")

# Bloque de manejo de errores
# ---------------------------
except FileNotFoundError:
    # Este error ocurre si la ruta o el nombre del archivo en la CONFIGURACIÓN son incorrectos.
    print("\n--- ¡ERROR CRÍTICO! ---")
    print(f"No se pudo encontrar el archivo en la ruta especificada: '{ruta_completa_original}'.")
    print("\nPor favor, verifica lo siguiente:")
    print("1. ¿La ruta de la carpeta es correcta?")
    print(f"2. ¿Existe un archivo llamado '{os.path.basename(ruta_completa_original)}' en esa carpeta?")
    print("3. Revisa si hay errores de tipeo en la ruta o el nombre del archivo.")
    
except Exception as e:
    # Captura cualquier otro error inesperado que pueda ocurrir durante el proceso.
    print(f"Ocurrió un error inesperado: {e}")