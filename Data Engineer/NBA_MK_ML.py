import streamlit as st
import pandas as pd
import numpy as np
import io
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score
import warnings

warnings.filterwarnings("ignore")

# --- CONFIGURACIÓN DE LA APLICACIÓN ---
st.set_page_config(
    page_title="Análisis de Marketing NBA con Clustering",
    layout="wide",
    page_icon="🏀",
    initial_sidebar_state="expanded",
)

# --- DATOS ---
DATOS_CSV = """team_city,team_name,season,wins,losses,win_percentage,playoff_appearance,championship_winner,all_star_players,avg_attendance,team_valuation_billions,city_population_millions,median_household_income
Atlanta Hawks,Atlanta Hawks,2020-2021,41,31,0.569,1,0,1,3085,1.70,6.1,65381
Boston Celtics,Boston Celtics,2020-2021,36,36,0.500,1,0,2,0,3.55,5.0,86535
Brooklyn Nets,Brooklyn Nets,2020-2021,48,24,0.667,1,0,3,1732,3.50,19.2,74693
Charlotte Hornets,Charlotte Hornets,2020-2021,33,39,0.458,0,0,0,2874,1.55,2.7,63486
Chicago Bulls,Chicago Bulls,2020-2021,31,41,0.431,0,0,1,0,3.65,9.5,74419
Cleveland Cavaliers,Cleveland Cavaliers,2020-2021,22,50,0.306,0,0,0,1944,1.65,2.1,61453
Dallas Mavericks,Dallas Mavericks,2020-2021,42,30,0.583,1,0,1,3486,2.70,7.9,72862
Denver Nuggets,Denver Nuggets,2020-2021,47,25,0.653,1,0,1,0,1.73,3.0,78873
Detroit Pistons,Detroit Pistons,2020-2021,20,52,0.278,0,0,0,0,1.58,4.4,63523
Golden State Warriors,Golden State Warriors,2020-2021,39,33,0.542,0,0,1,0,7.00,4.7,121757
Houston Rockets,Houston Rockets,2020-2021,17,55,0.236,0,0,0,3177,2.75,7.3,71146
Indiana Pacers,Indiana Pacers,2020-2021,34,38,0.472,0,0,1,1631,1.67,2.1,64177
Los Angeles Clippers,Los Angeles Clippers,2020-2021,47,25,0.653,1,0,2,0,3.90,13.2,77827
Los Angeles Lakers,Los Angeles Lakers,2020-2021,42,30,0.583,1,0,2,0,5.90,13.2,77827
Memphis Grizzlies,Memphis Grizzlies,2020-2021,38,34,0.528,1,0,0,2337,1.50,1.4,59539
Miami Heat,Miami Heat,2020-2021,40,32,0.556,1,0,0,1938,2.30,6.1,60822
Milwaukee Bucks,Milwaukee Bucks,2020-2021,46,26,0.639,1,1,1,1734,2.30,1.6,60278
Minnesota Timberwolves,Minnesota Timberwolves,2020-2021,23,49,0.319,0,0,0,0,1.53,3.7,81333
New Orleans Pelicans,New Orleans Pelicans,2020-2021,31,41,0.431,0,0,1,1657,1.60,1.3,54634
New York Knicks,New York Knicks,2020-2021,41,31,0.569,1,0,1,1822,6.10,19.2,74693
Oklahoma City Thunder,Oklahoma City Thunder,2020-2021,22,50,0.306,0,0,0,0,1.63,1.4,59194
Orlando Magic,Orlando Magic,2020-2021,21,51,0.292,0,0,0,3159,1.64,2.7,61461
Philadelphia 76ers,Philadelphia 76ers,2020-2021,49,23,0.681,1,0,1,3071,2.65,6.1,73256
Phoenix Suns,Phoenix Suns,2020-2021,51,21,0.708,1,0,1,3156,1.80,5.0,68326
Portland Trail Blazers,Portland Trail Blazers,2020-2021,42,30,0.583,1,0,1,0,2.05,2.5,78284
Sacramento Kings,Sacramento Kings,2020-2021,31,41,0.431,0,0,0,0,2.10,2.4,76326
San Antonio Spurs,San Antonio Spurs,2020-2021,33,39,0.458,0,0,0,3156,2.00,2.6,63953
Toronto Raptors,Toronto Raptors,2020-2021,27,45,0.375,0,0,1,381,2.15,6.3,81652
Utah Jazz,Utah Jazz,2020-2021,52,20,0.722,1,0,2,5547,1.75,1.2,73456
Washington Wizards,Washington Wizards,2020-2021,34,38,0.472,1,0,1,0,1.93,6.3,101072
Atlanta Hawks,Atlanta Hawks,2022-2023,41,41,0.500,1,0,1,17414,2.60,6.1,65381
Boston Celtics,Boston Celtics,2022-2023,57,25,0.695,1,0,2,19156,4.00,5.0,86535
Brooklyn Nets,Brooklyn Nets,2022-2023,45,37,0.549,1,0,1,17732,3.85,19.2,74693
Charlotte Hornets,Charlotte Hornets,2022-2023,27,55,0.329,0,0,1,17122,1.70,2.7,63486
Chicago Bulls,Chicago Bulls,2022-2023,40,42,0.488,1,0,1,20577,4.10,9.5,74419
Cleveland Cavaliers,Cleveland Cavaliers,2022-2023,51,31,0.622,1,0,1,19432,1.90,2.1,61453
Dallas Mavericks,Dallas Mavericks,2022-2023,38,44,0.463,0,0,1,19737,3.30,7.9,72862
Denver Nuggets,Denver Nuggets,2022-2023,53,29,0.646,1,1,1,19538,2.73,3.0,78873
Detroit Pistons,Detroit Pistons,2022-2023,17,65,0.207,0,0,0,19515,1.90,4.4,63523
Golden State Warriors,Golden State Warriors,2022-2023,44,38,0.537,1,0,2,18064,7.55,4.7,121757
Houston Rockets,Houston Rockets,2022-2023,22,60,0.268,0,0,0,16568,3.10,7.3,71146
Indiana Pacers,Indiana Pacers,2022-2023,35,47,0.427,0,0,1,17045,2.13,2.1,64177
Los Angeles Clippers,Los Angeles Clippers,2022-2023,44,38,0.537,1,0,1,19068,4.25,13.2,77827
Los Angeles Lakers,Los Angeles Lakers,2022-2023,43,39,0.524,1,0,2,18997,6.40,13.2,77827
Memphis Grizzlies,Memphis Grizzlies,2022-2023,51,31,0.622,1,0,2,17794,1.80,1.4,59539
Miami Heat,Miami Heat,2022-2023,44,38,0.537,1,0,1,19628,3.00,6.1,60822
Milwaukee Bucks,Milwaukee Bucks,2022-2023,58,24,0.707,1,0,2,17341,2.70,1.6,60278
Minnesota Timberwolves,Minnesota Timberwolves,2022-2023,42,40,0.512,1,0,1,17290,1.75,3.7,81333
New Orleans Pelicans,New Orleans Pelicans,2022-2023,42,40,0.512,1,0,1,17144,1.85,1.3,54634
New York Knicks,New York Knicks,2022-2023,47,35,0.573,1,0,1,19812,6.55,19.2,74693
Oklahoma City Thunder,Oklahoma City Thunder,2022-2023,40,42,0.488,1,0,1,18203,2.00,1.4,59194
Orlando Magic,Orlando Magic,2022-2023,34,48,0.415,0,0,1,17983,1.88,2.7,61461
Philadelphia 76ers,Philadelphia 76ers,2022-2023,54,28,0.659,1,0,1,20626,3.50,6.1,73256
Phoenix Suns,Phoenix Suns,2022-2023,45,37,0.549,1,0,2,17071,2.70,5.0,68326
Portland Trail Blazers,Portland Trail Blazers,2022-2023,33,49,0.402,0,0,1,18073,2.30,2.5,78284
Sacramento Kings,Sacramento Kings,2022-2023,48,34,0.585,1,0,2,17608,2.33,2.4,76326
San Antonio Spurs,San Antonio Spurs,2022-2023,22,60,0.268,0,0,0,15687,2.20,2.6,63953
Toronto Raptors,Toronto Raptors,2022-2023,41,41,0.500,1,0,1,19815,2.45,6.3,81652
Utah Jazz,Utah Jazz,2022-2023,37,45,0.451,0,0,1,18206,2.18,1.2,73456
Washington Wizards,Washington Wizards,2022-2023,35,47,0.427,0,0,1,17336,2.50,6.3,101072
"""


# --- FUNCIONES AUXILIARES ---
@st.cache_data
def cargar_y_procesar_datos():
    """Carga y procesa los datos del CSV"""
    df = pd.read_csv(io.StringIO(DATOS_CSV))

    # Filtrar solo datos de 2022-2023
    df_reciente = df[df["season"] == "2022-2023"].copy()

    # Limpiar datos de asistencia (convertir 0 a NaN para mejor manejo)
    df_reciente["avg_attendance"] = df_reciente["avg_attendance"].replace(0, np.nan)
    df_reciente["avg_attendance"] = df_reciente["avg_attendance"].fillna(
        df_reciente["avg_attendance"].median()
    )

    # Características para clustering
    features = [
        "win_percentage",
        "all_star_players",
        "avg_attendance",
        "team_valuation_billions",
        "city_population_millions",
        "median_household_income",
    ]

    # Crear nombres más amigables para mostrar
    feature_names = {
        "win_percentage": "Porcentaje de Victorias",
        "all_star_players": "Jugadores All-Star",
        "avg_attendance": "Asistencia Promedio",
        "team_valuation_billions": "Valoración (Miles de Millones)",
        "city_population_millions": "Población Ciudad (Millones)",
        "median_household_income": "Ingreso Mediano Familiar",
    }

    df_features = df_reciente[features]

    # Escalado de datos
    scaler = StandardScaler()
    df_scaled = scaler.fit_transform(df_features)

    return df_reciente, df_scaled, features, feature_names, scaler


def encontrar_numero_optimo_clusters(data, max_k=8):
    """Encuentra el número óptimo de clusters usando el método del codo y silhouette score"""
    inertias = []
    silhouette_scores = []
    k_range = range(2, max_k + 1)

    for k in k_range:
        kmeans = KMeans(n_clusters=k, init="k-means++", n_init=10, random_state=42)
        kmeans.fit(data)
        inertias.append(kmeans.inertia_)
        silhouette_scores.append(silhouette_score(data, kmeans.labels_))

    return k_range, inertias, silhouette_scores


def crear_nombres_tiers(cluster_profiles, n_clusters):
    """Crea nombres descriptivos para los tiers basados en sus características"""
    tier_names = {}

    # Ordenar clusters por valoración promedio
    sorted_clusters = cluster_profiles.sort_values(
        "team_valuation_billions", ascending=False
    )

    for i, cluster_id in enumerate(sorted_clusters.index):
        if i == 0:
            tier_names[cluster_id] = f"🏆 Tier Elite (Cluster {cluster_id})"
        elif i == 1 and n_clusters > 2:
            tier_names[cluster_id] = f"⭐ Tier Premium (Cluster {cluster_id})"
        elif i == n_clusters - 1:
            tier_names[cluster_id] = f"🔧 Tier Desarrollo (Cluster {cluster_id})"
        else:
            tier_names[cluster_id] = f"📈 Tier Crecimiento (Cluster {cluster_id})"

    return tier_names


# --- CARGA DE DATOS ---
df, df_scaled, features, feature_names, scaler = cargar_y_procesar_datos()

# --- INTERFAZ PRINCIPAL ---
st.title("🏀 Análisis de Mercados NBA con Machine Learning")
st.markdown(
    """
### Dashboard Inteligente de Clustering
Este análisis utiliza **K-Means clustering** para segmentar automáticamente los equipos NBA 
en diferentes tiers de mercado, identificando oportunidades de inversión y patrones estratégicos.
"""
)

# --- SIDEBAR ---
st.sidebar.header("⚙️ Configuración del Modelo")

# Opción para mostrar análisis de clusters óptimos
show_optimization = st.sidebar.checkbox(
    "🔍 Mostrar análisis de clusters óptimos",
    help="Muestra gráficos para ayudar a determinar el número ideal de clusters",
)

if show_optimization:
    with st.spinner("Calculando número óptimo de clusters..."):
        k_range, inertias, silhouette_scores = encontrar_numero_optimo_clusters(
            df_scaled
        )

    st.sidebar.subheader("Análisis de Clusters Óptimos")

    # Método del codo
    fig_elbow = go.Figure()
    fig_elbow.add_trace(
        go.Scatter(
            x=list(k_range),
            y=inertias,
            mode="lines+markers",
            name="Inercia",
            line=dict(color="blue", width=3),
            marker=dict(size=8),
        )
    )
    fig_elbow.update_layout(
        title="Método del Codo",
        xaxis_title="Número de Clusters (k)",
        yaxis_title="Inercia",
        height=300,
    )
    st.sidebar.plotly_chart(fig_elbow, use_container_width=True)

    # Silhouette Score
    fig_sil = go.Figure()
    fig_sil.add_trace(
        go.Scatter(
            x=list(k_range),
            y=silhouette_scores,
            mode="lines+markers",
            name="Silhouette Score",
            line=dict(color="green", width=3),
            marker=dict(size=8),
        )
    )
    fig_sil.update_layout(
        title="Silhouette Score",
        xaxis_title="Número de Clusters (k)",
        yaxis_title="Silhouette Score",
        height=300,
    )
    st.sidebar.plotly_chart(fig_sil, use_container_width=True)

    # Recomendación automática
    best_k = k_range[np.argmax(silhouette_scores)]
    st.sidebar.success(f"🎯 Número óptimo recomendado: **{best_k} clusters**")

# Selector de número de clusters
n_clusters = st.sidebar.slider(
    "📊 Número de Tiers/Clusters:",
    min_value=2,
    max_value=8,
    value=4,
    help="Selecciona en cuántos grupos segmentar el mercado NBA",
)

# Selector de características
st.sidebar.subheader("🎯 Características del Modelo")
selected_features = st.sidebar.multiselect(
    "Selecciona las características para el clustering:",
    options=features,
    default=features,
    format_func=lambda x: feature_names[x],
)

if not selected_features:
    st.error("❌ Debes seleccionar al menos una característica para el análisis.")
    st.stop()

# --- EJECUCIÓN DEL CLUSTERING ---
# Filtrar datos según características seleccionadas
feature_indices = [features.index(f) for f in selected_features]
df_scaled_filtered = df_scaled[:, feature_indices]

# Ejecutar K-Means
kmeans = KMeans(n_clusters=n_clusters, init="k-means++", n_init=10, random_state=42)
clusters = kmeans.fit_predict(df_scaled_filtered)
df["cluster"] = clusters

# Calcular métricas del modelo
silhouette_avg = silhouette_score(df_scaled_filtered, clusters)

# --- RESULTADOS PRINCIPALES ---
col1, col2, col3 = st.columns(3)

with col1:
    st.metric("📊 Número de Clusters", n_clusters)

with col2:
    st.metric("🎯 Silhouette Score", f"{silhouette_avg:.3f}")

with col3:
    st.metric("📈 Características Usadas", len(selected_features))

# Crear perfiles de clusters
cluster_profiles = df.groupby("cluster")[selected_features].mean()
tier_names = crear_nombres_tiers(cluster_profiles, n_clusters)

# --- ANÁLISIS DE TIERS ---
st.header("🏆 Análisis de Tiers de Mercado")

# Identificar el tier elite
elite_cluster = cluster_profiles.sort_values(
    "team_valuation_billions", ascending=False
).index[0]
equipos_elite = df[df["cluster"] == elite_cluster]["team_name"].tolist()

st.success(
    f"""
### 🎯 Recomendación de Inversión Principal
**{tier_names[elite_cluster]}** representa la mejor oportunidad de mercado.

**Equipos en este tier:** {', '.join(equipos_elite)}
"""
)

# Mostrar todos los tiers
for cluster_id in sorted(cluster_profiles.index):
    equipos_cluster = df[df["cluster"] == cluster_id]

    with st.expander(f"{tier_names[cluster_id]} - {len(equipos_cluster)} equipos"):
        col1, col2 = st.columns([1, 2])

        with col1:
            st.write("**Equipos:**")
            for equipo in equipos_cluster["team_name"]:
                st.write(f"• {equipo}")

        with col2:
            st.write("**Características Promedio:**")
            for feature in selected_features:
                valor = cluster_profiles.loc[cluster_id, feature]
                st.write(f"• {feature_names[feature]}: {valor:.3f}")

# --- VISUALIZACIONES ---
st.header("📊 Visualizaciones Interactivas")

# Tabs para diferentes visualizaciones
tab1, tab2, tab3, tab4 = st.tabs(
    [
        "🗺️ Mapa PCA",
        "📈 Perfiles de Clusters",
        "🎯 Análisis por Característica",
        "📋 Tabla Detallada",
    ]
)

with tab1:
    st.subheader("Mapa de Posicionamiento (PCA)")

    # PCA para visualización
    pca = PCA(n_components=2)
    df_pca = pca.fit_transform(df_scaled_filtered)

    # Crear DataFrame para plotly
    df_plot = df.copy()
    df_plot["PCA1"] = df_pca[:, 0]
    df_plot["PCA2"] = df_pca[:, 1]
    df_plot["Tier"] = df_plot["cluster"].map(tier_names)

    # Gráfico interactivo
    fig_pca = px.scatter(
        df_plot,
        x="PCA1",
        y="PCA2",
        color="Tier",
        hover_data=["team_name", "team_valuation_billions", "win_percentage"],
        title="Mapa de Posicionamiento de Equipos NBA",
        labels={
            "PCA1": f"Componente 1 ({pca.explained_variance_ratio_[0]:.1%} varianza)",
            "PCA2": f"Componente 2 ({pca.explained_variance_ratio_[1]:.1%} varianza)",
        },
    )
    fig_pca.update_traces(marker=dict(size=12))
    fig_pca.update_layout(height=600)

    st.plotly_chart(fig_pca, use_container_width=True)

    st.info(
        f"""
    **Interpretación del PCA:**
    - Componente 1 explica {pca.explained_variance_ratio_[0]:.1%} de la varianza
    - Componente 2 explica {pca.explained_variance_ratio_[1]:.1%} de la varianza
    - Total explicado: {sum(pca.explained_variance_ratio_):.1%}
    """
    )

with tab2:
    st.subheader("Perfiles Comparativos de Clusters")

    # Gráfico de radar
    fig_radar = go.Figure()

    for cluster_id in cluster_profiles.index:
        values = []
        for feature in selected_features:
            # Normalizar valores para el radar (0-1)
            min_val = df[feature].min()
            max_val = df[feature].max()
            normalized_val = (cluster_profiles.loc[cluster_id, feature] - min_val) / (
                max_val - min_val
            )
            values.append(normalized_val)

        # Cerrar el polígono
        values += values[:1]
        feature_labels = [feature_names[f] for f in selected_features] + [
            feature_names[selected_features[0]]
        ]

        fig_radar.add_trace(
            go.Scatterpolar(
                r=values,
                theta=feature_labels,
                fill="toself",
                name=tier_names[cluster_id],
            )
        )

    fig_radar.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
        showlegend=True,
        title="Perfil Comparativo de Tiers (Valores Normalizados)",
        height=600,
    )

    st.plotly_chart(fig_radar, use_container_width=True)

with tab3:
    st.subheader("Análisis por Característica Individual")

    # Selector de característica
    selected_feature = st.selectbox(
        "Selecciona una característica para analizar:",
        options=selected_features,
        format_func=lambda x: feature_names[x],
    )

    # Box plot
    df_plot = df.copy()
    df_plot["Tier"] = df_plot["cluster"].map(tier_names)

    fig_box = px.box(
        df_plot,
        x="Tier",
        y=selected_feature,
        title=f"Distribución de {feature_names[selected_feature]} por Tier",
        points="all",
    )
    fig_box.update_layout(height=500)

    st.plotly_chart(fig_box, use_container_width=True)

    # Estadísticas descriptivas
    st.write("**Estadísticas por Tier:**")
    stats_df = (
        df.groupby("cluster")[selected_feature]
        .agg(["mean", "std", "min", "max"])
        .round(3)
    )
    stats_df.index = [tier_names[i] for i in stats_df.index]
    st.dataframe(stats_df)

with tab4:
    st.subheader("Tabla Detallada de Resultados")

    # Preparar tabla final
    df_display = df[["team_name", "team_city", "cluster"] + selected_features].copy()
    df_display["Tier"] = df_display["cluster"].map(tier_names)
    df_display = df_display.drop("cluster", axis=1)

    # Renombrar columnas
    column_mapping = {"team_name": "Equipo", "team_city": "Ciudad"}
    column_mapping.update({f: feature_names[f] for f in selected_features})
    df_display = df_display.rename(columns=column_mapping)

    # Ordenar por tier y luego por valoración
    df_display = df_display.sort_values(
        ["Tier", "Valoración (Miles de Millones)"], ascending=[True, False]
    )

    st.dataframe(df_display, use_container_width=True, height=600)

    # Opción de descarga
    csv = df_display.to_csv(index=False)
    st.download_button(
        label="📥 Descargar resultados como CSV",
        data=csv,
        file_name="analisis_nba_clustering.csv",
        mime="text/csv",
    )

# --- INSIGHTS Y RECOMENDACIONES ---
st.header("💡 Insights y Recomendaciones Estratégicas")

# Análisis automático de insights
elite_profile = cluster_profiles.loc[elite_cluster]
all_mean = df[selected_features].mean()

insights = []

# Comparar tier elite con promedio general
for feature in selected_features:
    if elite_profile[feature] > all_mean[feature] * 1.2:  # 20% por encima del promedio
        insights.append(
            f"• El tier elite destaca significativamente en **{feature_names[feature]}** ({elite_profile[feature]:.2f} vs {all_mean[feature]:.2f} promedio)"
        )

if insights:
    st.success("**Características distintivas del Tier Elite:**")
    for insight in insights:
        st.write(insight)

# Recomendaciones generales
st.info(
    """
### 🎯 Recomendaciones Estratégicas:

1. **Inversión Prioritaria**: Enfócate en los equipos del tier elite para maximizar ROI
2. **Oportunidades de Crecimiento**: Los tiers medios pueden ofrecer mejor relación valor-potencial
3. **Mercados Emergentes**: Analiza los equipos en desarrollo para inversiones a largo plazo
4. **Diversificación**: Considera una cartera balanceada entre diferentes tiers según tu estrategia de riesgo
"""
)

# Footer
st.markdown("---")
st.markdown("*Dashboard desarrollado con Streamlit, Scikit-learn y Plotly* 🚀")
