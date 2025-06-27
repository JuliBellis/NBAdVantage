# %%
import pandas as pd
import numpy as np


# %%
df= pd.read_csv("game_summary.csv")

# %%
print(df['game_id'].nunique())

# %%
df_filtrado = df[df['season'] >= 2021]

print(df_filtrado)

# %%
print("--- Filas duplicadas (a partir de la segunda ocurrencia) ---")
duplicados_first = df_filtrado.duplicated()
print(duplicados_first)
print("\n")


print("--- Solo las filas marcadas como duplicadas (keep='first') ---")
print(df_filtrado[duplicados_first])
print("\n")




# %%
df_sin_duplicados = df_filtrado.drop_duplicates(keep='first')

# %% [markdown]
# control de duplicados

# %%
duplicados_first = df_sin_duplicados.duplicated()
print(duplicados_first)
print("\n")

# %% [markdown]
# Porcentage de vacios/trato vacios

# %%
porcentaje_vacios = df_sin_duplicados.isnull().mean() * 100
print(porcentaje_vacios)

# %% [markdown]
# Elimino las columnas que no usaremos 

# %%
df_sin_duplicados = df_sin_duplicados.drop(columns=["live_pc_time", "natl_tv_broadcaster_abbreviation"])

# %% [markdown]
# Controlo estado final

# %%
df_sin_duplicados.info

# %%
df_sin_duplicados.to_csv("game_sumary_ok.csv", index=False)


