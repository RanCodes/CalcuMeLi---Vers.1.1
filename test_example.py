import pandas as pd
import io
from math import isclose
from data_processor import leer_ml, leer_odoo, unir_y_validar, calcular, preparar_resultado_final, exportar_excel

def crear_datos_ejemplo():
    """
    Crea datos de ejemplo basados en la estructura real de los archivos.
    """
    # Datos de ejemplo MercadoLibre
    ml_data = {
        'ITEM_ID': ['MLA934071512', 'MLA864175834', 'MLA123456789', 'MLA987654321', 'MLA555666777'],
        'VARIATION_ID': ['94135189655', '175837150784', None, '123456789', None],
        'SKU': ['LED7012795', 'CORNPR06WW', 'TCL45310', 'MMM42385', 'NOEXISTE123'],
        'TITLE': [
            'Lampara Sodio 250w E40 Osram Alumbrado Público',
            'Macroled Panel Plafón Redondo Led 6w Cálido Black Npr06',
            'Módulo ciego BLANCO',
            '3M™ Cinta de Empaque 301 - 48mm x 50m',
            'Producto sin match en Odoo'
        ],
        'QUANTITY': [232, 106, 50, 200, 10],
        'PRICE': [21997.46, 8164.02, 250.00, 2500.00, 1000.00],
        'CURRENCY_ID': ['$', '$', '$', '$', '$'],
        'FEE_PER_SALE_MARKETPLACE_V2': [
            '14.50% + $1095.00',
            '12.00% + $800.00',
            '15.00% + $500.00',
            '13.50% + $750.00',
            '16.00% + $1200.00'
        ],
        'COST_OF_FINANCING_MARKETPLACE': ['4.00%', '3.50%', '0.00%', '5.00%', '4.50%'],
        'LISTING_TYPE_V3': ['gold_special', 'gold_pro', 'free', 'gold_special', 'gold_pro']
    }

    # Datos de ejemplo Odoo
    odoo_data = {
        'Código Neored': ['LED7012795', 'CORNPR06WW', 'TCL45310', 'MMM42385', 'EXTRA12345'],
        'Nombre': [
            'Lámpara Sodio 250W E40 Osram',
            'Panel LED Redondo 6W Cálido',
            '1/2 Módulo ciego BLANCO',
            '3M™ Cinta de Empaque 301 - 48mm x 50m',
            'Producto extra sin match en ML'
        ],
        'Cantidad a mano': [250, 120, 60, 576, 100],
        'Precio Tarifa': [18500.00, 6800.00, 184.05, 1915.72, 2000.00],
        'Impuestos del cliente': ['IVA Ventas 21%', 'IVA Ventas 21%', 'IVA Ventas 21%', 'IVA Ventas 21%', 'IVA Ventas 21%']
    }

    df_ml = pd.DataFrame(ml_data)
    df_odoo = pd.DataFrame(odoo_data)
    return df_ml, df_odoo

def test_procesamiento():
    """
    Prueba el procesamiento completo con datos de ejemplo.
    """
    print("🧪 Iniciando prueba con datos de ejemplo")
    print("=" * 50)

    df_ml, df_odoo = crear_datos_ejemplo()
    print("📊 Datos de entrada creados:")
    print(f"- MercadoLibre: {len(df_ml)} items")
    print(f"- Odoo: {len(df_odoo)} productos")
    print("\n🔄 Simulando procesamiento...")
    from utils import parse_fee_combo, parse_pct
    df_ml['fee_pct'], df_ml['fee_fixed'] = zip(*df_ml['FEE_PER_SALE_MARKETPLACE_V2'].apply(parse_fee_combo))
    df_ml['financing_pct'] = df_ml['COST_OF_FINANCING_MARKETPLACE'].apply(parse_pct)
    from utils import extract_tax_percentage
    df_odoo['tax_pct'] = df_odoo['Impuestos del cliente'].apply(extract_tax_percentage)
    print("✅ Campos parseados correctamente")
    print("\n🔗 Uniendo datos por SKU...")
    df_merged = unir_y_validar(df_ml, df_odoo)
    total_items = len(df_merged)
    matched_items = len(df_merged[df_merged['Código Neored'].notna()])
    print(f"- Total items: {total_items}")
    print(f"- Matches encontrados: {matched_items}")
    print(f"- Tasa de match: {matched_items/total_items*100:.1f}%")
    no_match = df_merged[df_merged['Código Neored'].isna()][['SKU', 'TITLE']]
    if not no_match.empty:
        print("\n⚠️ Items sin match:")
        for idx, row in no_match.iterrows():
            print(f"  - {row['SKU']}: {row['TITLE'][:50]}...")
    print("\n💰 Calculando precios (base: tarifa, sin impuestos)...")
    df_calculated = calcular(df_merged, base_financiacion='tarifa', incluir_impuestos=False)
    assert 'VALUE_ADDED_TAX' in df_calculated.columns
    producto_led = df_calculated[df_calculated['SKU'] == 'LED7012795'].iloc[0]
    assert isclose(producto_led['VALUE_ADDED_TAX'], 4833.68, rel_tol=1e-04)
    assert isclose(producto_led['Precio final'], 27851.17, rel_tol=1e-04)
    print("\n📋 Preparando resultado final...")
    df_resultado = preparar_resultado_final(df_calculated, incluir_impuestos=False)
    assert 'VALUE_ADDED_TAX' in df_resultado.columns
    resultado_led = df_resultado[df_resultado['SKU'] == 'LED7012795'].iloc[0]
    assert isclose(resultado_led['VALUE_ADDED_TAX'], 4833.68, rel_tol=1e-04)
    assert isclose(resultado_led['Precio final'], 27851.17, rel_tol=1e-04)
    print("✅ Procesamiento completado")
    print("\n📊 RESULTADOS DETALLADOS:")
    print("=" * 50)
    for idx, row in df_resultado.iterrows():
        if row['Precio final'] > 0:
            print(f"\n🏷️ SKU: {row['SKU']}")
            print(f"📦 Producto: {row['Descripción del producto'][:60]}")
            print(f"📊 Stock: {row['Stock']} unidades")
            print(f"💵 Precio Tarifa: ${row['Precio de Tarifa']:,.2f}")
            print(f"🎯 Precio Final: ${row['Precio final']:,.2f}")
            print("📈 Desglose:")
            print(f"   - Recargo % ML ({row['% ML aplicado']:.1f}%): ${row['Recargo % ML (importe)']:,.2f}")
            print(f"   - Recargo fijo ML: ${row['Recargo fijo ML ($)']:,.2f}")
            print(f"   - Recargo financiación ({row['% financiación aplicado']:.1f}%): ${row['Recargo financiación (importe)']:,.2f}")
            if 'Recargo envío ($)' in df_resultado.columns:
                print(f"   - Recargo envío: ${row.get('Recargo envío ($)', 0):,.2f}")
            print(f"📋 Tipo: {row['Tipo de publicación']}")
            if row['Notas/Flags']:
                print(f"⚠️ Advertencias: {row['Notas/Flags']}")
    print("\n" + "=" * 50)
    print("🧪 PRUEBA CON IMPUESTOS INCLUIDOS")
    print("=" * 50)
    df_calculated_tax = calcular(df_merged, base_financiacion='tarifa', incluir_impuestos=True)
    assert 'VALUE_ADDED_TAX' in df_calculated_tax.columns
    producto_led_tax = df_calculated_tax[df_calculated_tax['SKU'] == 'LED7012795'].iloc[0]
    assert isclose(producto_led_tax['VALUE_ADDED_TAX'], 5800.46, rel_tol=1e-04)
    assert isclose(producto_led_tax['Precio final'], 33421.68, rel_tol=1e-04)
    df_resultado_tax = preparar_resultado_final(df_calculated_tax, incluir_impuestos=True)
    assert 'VALUE_ADDED_TAX' in df_resultado_tax.columns
    resultado_led_tax = df_resultado_tax[df_resultado_tax['SKU'] == 'LED7012795'].iloc[0]
    assert isclose(resultado_led_tax['VALUE_ADDED_TAX'], 5800.46, rel_tol=1e-04)
    assert isclose(resultado_led_tax['Precio final'], 33421.68, rel_tol=1e-04)
    print("\n📊 Comparación con y sin impuestos (primeros 3 items):")
    comparacion_cols = ['SKU', 'Precio de Tarifa', 'Tarifa + impuestos', 'Precio final']
    df_comp = df_resultado_tax[df_resultado_tax['Precio final'] > 0][comparacion_cols].head(3)
    for idx, row in df_comp.iterrows():
        print(f"\n🏷️ SKU: {row['SKU']}")
        print(f"💵 Tarifa base: ${row['Precio de Tarifa']:,.2f}")
        print(f"💰 Tarifa + IVA: ${row['Tarifa + impuestos']:,.2f}")
        print(f"🎯 Precio final: ${row['Precio final']:,.2f}")
        aumento = ((row['Precio final'] / row['Precio de Tarifa']) - 1) * 100
        print(f"📈 Aumento total: {aumento:.1f}%")
    print(f"\n💾 Generando archivo Excel de ejemplo...")
    excel_bytes = exportar_excel(df_resultado)
    with open('ML_precios_y_stock_calculados_EJEMPLO.xlsx', 'wb') as f:
        f.write(excel_bytes)
    print("✅ Archivo generado: ML_precios_y_stock_calculados_EJEMPLO.xlsx")
    print(f"\n📈 RESUMEN ESTADÍSTICO:")
    print("=" * 30)
    items_validos = df_resultado[df_resultado['Precio final'] > 0]
    if not items_validos.empty:
        print(f"Items procesados: {len(items_validos)}")
        print(f"Precio promedio: ${items_validos['Precio final'].mean():,.2f}")
        print(f"Precio mínimo: ${items_validos['Precio final'].min():,.2f}")
        print(f"Precio máximo: ${items_validos['Precio final'].max():,.2f}")
        recargo_total = (
            items_validos['Recargo % ML (importe)'] +
            items_validos['Recargo fijo ML ($)'] +
            items_validos['Recargo financiación (importe)']
        )
        if 'Recargo envío ($)' in items_validos.columns:
            recargo_total = recargo_total + items_validos['Recargo envío ($)']
        print(f"\nRecargo promedio ML: ${recargo_total.mean():,.2f}")
        porcentaje_recargo = (recargo_total / items_validos['Precio de Tarifa']).mean() * 100
        print(f"Recargo promedio %: {porcentaje_recargo:.1f}%")
    print("\n🎉 ¡Prueba completada exitosamente!")
    return df_resultado

def test_parseo_individual():
    """
    Prueba las funciones de parseo individualmente.
    """
    from utils import parse_money, parse_pct, parse_fee_combo
    print("\n🔧 PRUEBA DE FUNCIONES DE PARSEO:")
    print("=" * 40)
    print("\n💰 Pruebas parse_money:")
    money_tests = [
        "$1,095.00", "1095", "1.095,50", "$2.500,75", "0", "", None
    ]
    for test in money_tests:
        result = parse_money(test)
        print(f"  '{test}' -> {result}")
    print("\n📊 Pruebas parse_pct:")
    pct_tests = [
        "14.50%", "4.00%", "0.04", "4", "21%", "0", "", None
    ]
    for test in pct_tests:
        result = parse_pct(test)
        print(f"  '{test}' -> {result:.4f}")
    print("\n🔀 Pruebas parse_fee_combo:")
    combo_tests = [
        "14.50% + $1095.00",
        "12.00% + $800.00",
        "15.00%",
        "$500.00",
        "16% + $1,200.00",
        "",
        None
    ]
    for test in combo_tests:
        pct, fixed = parse_fee_combo(test)
        print(f"  '{test}' -> {pct:.4f}, ${fixed:.2f}")

if __name__ == "__main__":
    test_parseo_individual()
    test_procesamiento()
    print("\n✨ Todas las pruebas completadas. Revisa el archivo Excel generado!")