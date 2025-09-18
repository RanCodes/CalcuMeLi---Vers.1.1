import streamlit as st
import pandas as pd
import io
from data_processor import leer_ml, leer_odoo, unir_y_validar, calcular, preparar_resultado_final, exportar_excel

# Configurar página
st.set_page_config(
    page_title="ML Precios y Stock Calculator",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="collapsed",
)

def main():
    """
    Aplicación Streamlit que calcula precios y stock para ML a partir de
    archivos Excel de MercadoLibre y Odoo. Todas las opciones de configuración
    se muestran en una única página para evitar reinicios al navegar entre
    diferentes secciones.
    """
    st.title("💰 ML Precios y Stock Calculator")
    st.markdown("---")

    # Configuraciones de cálculo
    with st.expander("⚙️ Configuración", expanded=True):
        base_financiacion = st.radio(
            "Base para cálculo de financiación:",
            options=['tarifa', 'tarifa_mas_ml'],
            index=0,
            help="Tarifa: Solo sobre precio tarifa | Tarifa + ML: Sobre tarifa + recargos ML"
        )
        incluir_impuestos = st.checkbox(
            "Incluir 'Impuestos del cliente' en Tarifa",
            value=False,
            help="Si se activa, suma los impuestos del cliente al precio tarifa antes de calcular recargos"
        )
        st.markdown("#### 📦 Recargo de envío")
        tipo_recargo_envio = st.radio(
            "Tipo de recargo de envío:",
            options=['Ninguno', 'Fijo ($)', 'Porcentaje (%)'],
            index=0,
            help="Seleccione si el recargo de envío es fijo o porcentual sobre la tarifa"
        )
        valor_recargo_envio = 0.0
        if tipo_recargo_envio == 'Fijo ($)':
            valor_recargo_envio = st.number_input(
                "Valor del recargo de envío ($)",
                min_value=0.0,
                value=0.0,
                step=0.1,
                format="%.2f",
                help="Ingrese un valor fijo en pesos para sumar a cada producto"
            )
        elif tipo_recargo_envio == 'Porcentaje (%)':
            valor_recargo_envio = st.number_input(
                "Valor del recargo de envío (%)",
                min_value=0.0,
                max_value=100.0,
                value=0.0,
                step=0.1,
                format="%.2f",
                help="Ingrese un porcentaje para aplicar sobre la tarifa (0-100)"
            )

    st.markdown("### 📋 Instrucciones")
    st.markdown(
        """
        1. Sube el archivo de **MercadoLibre** (.xlsx)
        2. Sube el archivo de **Odoo** (.xlsx)
        3. Configura las opciones según necesites
        4. Haz clic en **'Calcular y exportar'**
        5. Descarga el resultado
        """
    )

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("📄 Archivo MercadoLibre")
        ml_file = st.file_uploader(
            "Sube el archivo MercadoLibre-cambiodeprecios-.xlsx",
            type=['xlsx', 'xls'],
            key="ml_file",
            help="Debe contener la hoja 'Hoja1' con las columnas requeridas"
        )
        if ml_file:
            st.success(f"✅ Archivo cargado: {ml_file.name}")
    with col2:
        st.subheader("📄 Archivo Odoo")
        odoo_file = st.file_uploader(
            "Sube el archivo Producto (product.template).xlsx",
            type=['xlsx', 'xls'],
            key="odoo_file",
            help="Debe contener la hoja 'Sheet1' con las columnas requeridas"
        )
        if odoo_file:
            st.success(f"✅ Archivo cargado: {odoo_file.name}")

    if ml_file and odoo_file:
        if st.button("🚀 Calcular y exportar", type="primary", use_container_width=True):
            with st.spinner("Procesando archivos..."):
                try:
                    st.info("📖 Leyendo archivo MercadoLibre...")
                    df_ml = leer_ml(ml_file)
                    st.success(f"✅ ML: {len(df_ml)} filas válidas encontradas")
                    st.info("📖 Leyendo archivo Odoo...")
                    df_odoo = leer_odoo(odoo_file)
                    st.success(f"✅ Odoo: {len(df_odoo)} productos encontrados")
                    st.info("🔗 Cruzando datos por SKU...")
                    df_merged = unir_y_validar(df_ml, df_odoo)
                    total_items = len(df_merged)
                    matched_items = len(df_merged[df_merged['Código Neored'].notna()])
                    match_rate = (matched_items / total_items * 100) if total_items > 0 else 0
                    col_a, col_b, col_c = st.columns(3)
                    with col_a:
                        st.metric("Total Items ML", total_items)
                    with col_b:
                        st.metric("SKUs Encontrados", matched_items)
                    with col_c:
                        st.metric("Tasa de Match", f"{match_rate:.1f}%")
                    if matched_items == 0:
                        st.error("❌ No se encontraron coincidencias de SKU entre los archivos.")
                        st.stop()
                    st.info("💰 Calculando precios finales...")
                    df_calculated = calcular(
                        df_merged,
                        base_financiacion=base_financiacion,
                        incluir_impuestos=incluir_impuestos,
                        tipo_recargo_envio=tipo_recargo_envio,
                        valor_recargo_envio=valor_recargo_envio
                    )
                    st.info("📊 Preparando resultado final...")
                    df_resultado = preparar_resultado_final(
                        df_calculated,
                        incluir_impuestos=incluir_impuestos,
                        incluir_envio=(tipo_recargo_envio != 'Ninguno')
                    )
                    st.success("✅ ¡Cálculo completado!")
                    items_con_precio = len(df_resultado[df_resultado['Precio final'] > 0])
                    items_con_errores = len(df_resultado[df_resultado['Notas/Flags'] != ''])
                    col_x, col_y, col_z = st.columns(3)
                    with col_x:
                        st.metric("Items Procesados", len(df_resultado))
                    with col_y:
                        st.metric("Con Precio Final", items_con_precio)
                    with col_z:
                        st.metric("Con Advertencias", items_con_errores)
                    st.subheader("👀 Vista previa del resultado")
                    st.dataframe(
                        df_resultado.head(20),
                        use_container_width=True,
                        hide_index=True
                    )
                    if len(df_resultado) > 20:
                        st.info(f"Mostrando las primeras 20 filas de {len(df_resultado)} totales")
                    if items_con_errores > 0:
                        st.subheader("⚠️ Resumen de advertencias")
                        warnings_df = df_resultado[df_resultado['Notas/Flags'] != ''][['SKU', 'Descripción del producto', 'Notas/Flags']]
                        st.dataframe(warnings_df, use_container_width=True, hide_index=True)
                    st.info("📤 Generando archivo Excel...")
                    excel_bytes = exportar_excel(df_resultado)
                    st.download_button(
                        label="📥 Descargar ML_precios_y_stock_calculados.xlsx",
                        data=excel_bytes,
                        file_name="ML_precios_y_stock_calculados.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        type="primary",
                        use_container_width=True
                    )
                    with st.expander("ℹ️ Información sobre el cálculo"):
                        st.markdown(f"""
                        **Configuración utilizada:**
                        - Base financiación: {base_financiacion.replace('_', ' + ').title()}
                        - Impuestos incluidos: {'Sí' if incluir_impuestos else 'No'}
                        - Recargo de envío: {tipo_recargo_envio} {valor_recargo_envio}

                        **Fórmula aplicada:**  
                        ```
                        Precio Final = Tarifa{' + impuestos' if incluir_impuestos else ''} 
                        + (Tarifa{' + impuestos' if incluir_impuestos else ''} × %ML) 
                        + Fijo ML 
                        + (Base_financiación × %Financiación) 
                        + Recargo envío
                        ```

                        **Donde Base_financiación es:**
                        - Tarifa: Solo precio tarifa {'(+ impuestos)' if incluir_impuestos else ''}
                        - Tarifa + ML: Tarifa + recargos ML {'(+ impuestos)' if incluir_impuestos else ''}
                        """)
                except Exception as e:
                    st.error(f"❌ Error al procesar archivos: {str(e)}")
                    st.exception(e)
    else:
        st.info("📁 Por favor, sube ambos archivos Excel para comenzar el procesamiento.")
        with st.expander("📋 Formato de archivos esperado"):
            col_left, col_right = st.columns(2)
            with col_left:
                st.markdown("""
                **Archivo MercadoLibre (Hoja1):**
                - ITEM_ID
                - VARIATION_ID
                - SKU
                - TITLE
                - QUANTITY
                - PRICE
                - CURRENCY_ID
                - FEE_PER_SALE_MARKETPLACE_V2
                - COST_OF_FINANCING_MARKETPLACE
                - LISTING_TYPE_V3
                """)
            with col_right:
                st.markdown("""
                **Archivo Odoo (Sheet1):**
                - Código Neored
                - Nombre
                - Cantidad a mano
                - Precio Tarifa
                - Impuestos del cliente
                """)

if __name__ == "__main__":
    main()