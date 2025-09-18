#!/usr/bin/env python3
"""
Script de prueba para ejecutar el pipeline con los EXCEL reales del proyecto.
Genera dos archivos de salida (config estándar y alternativa).
"""
import sys
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from data_processor import (
    leer_ml, leer_odoo, unir_y_validar, calcular, preparar_resultado_final, exportar_excel
)

def main():
    print("🚀 ML Precios Calculator - Prueba con archivos reales")
    print("=" * 70)

    # Rutas correctas (según los archivos incluidos en este proyecto)
    ml_file = BASE_DIR / "MercadoLibre-cambiodeprecios-.xlsx"
    odoo_file = BASE_DIR / "Producto (product.template) (1).xlsx"

    if not ml_file.exists():
        print(f"❌ No se encontró el archivo ML: {ml_file}")
        return 1
    if not odoo_file.exists():
        print(f"❌ No se encontró el archivo Odoo: {odoo_file}")
        return 1
    try:
        # Leer archivos
        print("📖 Leyendo Excel de MercadoLibre...")
        df_ml = leer_ml(ml_file)
        print(f"   → Filas válidas ML: {len(df_ml)}")
        print("📖 Leyendo Excel de Odoo...")
        df_odoo = leer_odoo(odoo_file)
        print(f"   → Productos Odoo: {len(df_odoo)}")
        # Unir
        print("🔗 Uniendo por SKU (Código Neored ↔ SKU)...")
        df_merged = unir_y_validar(df_ml, df_odoo)
        print(f"   → Filas tras join: {len(df_merged)}")
        # Calcular (modo estándar: financiación sobre TARIFA)
        print("💰 Calculando precios (base_financiacion='tarifa', incluir_impuestos=False)...")
        df_calc_std = calcular(df_merged, base_financiacion='tarifa', incluir_impuestos=False)
        df_res_std = preparar_resultado_final(df_calc_std, incluir_impuestos=False)
        out1 = BASE_DIR / "ML_precios_y_stock_calculados.xlsx"
        exportar_excel(df_res_std, output_path=str(out1))
        print(f"✅ Generado: {out1.name} ({len(df_res_std)} filas)")
        # Calcular (modo alternativo: financiación sobre TARIFA + %ML + FIJO)
        print("💰 Calculando precios (base_financiacion='tarifa_mas_ml', incluir_impuestos=False)...")
        df_calc_alt = calcular(df_merged, base_financiacion='tarifa_mas_ml', incluir_impuestos=False)
        df_res_alt = preparar_resultado_final(df_calc_alt, incluir_impuestos=False)
        out2 = BASE_DIR / "ML_precios_y_stock_calculados_alt.xlsx"
        exportar_excel(df_res_alt, output_path=str(out2))
        print(f"✅ Generado: {out2.name} ({len(df_res_alt)} filas)")
        # Resumen simple
        con_precio = (df_res_std["Precio final"] > 0).sum()
        con_flags = (df_res_std["Notas/Flags"] != "").sum()
        print("\n📊 Resumen (config estándar)")
        print(f"   Filas totales:       {len(df_res_std)}")
        print(f"   Con precio final >0: {con_precio}")
        print(f"   Con notas/flags:     {con_flags}")
        print("\n✨ Prueba completada. Revisa los Excel generados en la carpeta del proyecto.")
        return 0
    except Exception as e:
        import traceback
        print("❌ Error durante el procesamiento:")
        print(f"   {e}")
        traceback.print_exc()
        return 1

if __name__ == '__main__':
    raise SystemExit(main())