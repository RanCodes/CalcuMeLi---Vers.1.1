import re
import pandas as pd
import numpy as np
from typing import Tuple, Optional

def parse_money(text: str) -> float:
    """
    Parsea texto monetario a float.
    Ej: "$1,095.00", "1095", "1.095,50" -> 1095.0
    """
    if pd.isna(text) or text is None:
        return 0.0
    text = str(text).strip()
    if not text:
        return 0.0
    # Remover símbolo de moneda y espacios
    text = re.sub(r'[\$\s]', '', text)
    # Si contiene coma y punto, determinar cuál es decimal
    if ',' in text and '.' in text:
        if text.rfind('.') > text.rfind(','):
            text = text.replace(',', '')
        else:
            text = text.replace('.', '').replace(',', '.')
    elif ',' in text:
        parts = text.split(',')
        if len(parts) == 2 and len(parts[1]) <= 2:
            text = text.replace(',', '.')
        else:
            text = text.replace(',', '')
    try:
        return float(text)
    except ValueError:
        return 0.0

def parse_pct(text: str) -> float:
    """
    Parsea texto de porcentaje a decimal.
    Ej: "14.50%", "4.00%", "0.04", "4" -> 0.145, 0.04, 0.04, 0.04
    """
    if pd.isna(text) or text is None:
        return 0.0
    text = str(text).strip()
    if not text:
        return 0.0
    text = re.sub(r'[%\s]', '', text)
    text = text.replace(',', '.')
    try:
        value = float(text)
        if value > 1:
            return value / 100.0
        else:
            return value
    except ValueError:
        return 0.0

def parse_fee_combo(text: str) -> Tuple[float, float]:
    """
    Parsea el campo FEE_PER_SALE_MARKETPLACE_V2.
    Formato típico: "14.50% + $1095.00"
    Returns: (porcentaje_decimal, fijo_pesos)
    """
    if pd.isna(text) or text is None:
        return 0.0, 0.0
    text = str(text).strip()
    if not text:
        return 0.0, 0.0
    pct_match = re.search(r'([\d\.,]+)\s*%', text)
    pct_value = 0.0
    if pct_match:
        pct_value = parse_pct(pct_match.group(1) + '%')
    fixed_match = re.search(r'\$\s*([\d\.,]+)', text)
    fixed_value = 0.0
    if fixed_match:
        fixed_value = parse_money('$' + fixed_match.group(1))
    return pct_value, fixed_value

def clean_ml_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia el DataFrame de MercadoLibre:
    - Conserva solo filas donde ITEM_ID empiece con 'ML'
    - SKU no esté vacío
    - Elimina filas de encabezado inválidas
    """
    df_clean = df.copy()
    valid_mask = (
        df_clean['ITEM_ID'].notna() &
        df_clean['ITEM_ID'].astype(str).str.startswith('ML', na=False) &
        df_clean['SKU'].notna() &
        (df_clean['SKU'].astype(str).str.strip() != '')
    )
    df_clean = df_clean[valid_mask].reset_index(drop=True)
    return df_clean

def validate_excel_structure(df: pd.DataFrame, file_type: str) -> Tuple[bool, str]:
    """
    Valida que el Excel tenga las columnas requeridas.
    Args:
        df: DataFrame a validar
        file_type: 'ml' o 'odoo'
    Returns:
        (es_valido, mensaje_error)
    """
    if file_type == 'ml':
        required_cols = ['ITEM_ID', 'SKU', 'TITLE', 'QUANTITY', 'PRICE',
                        'CURRENCY_ID', 'FEE_PER_SALE_MARKETPLACE_V2',
                        'COST_OF_FINANCING_MARKETPLACE', 'LISTING_TYPE_V3']
    elif file_type == 'odoo':
        required_cols = ['Código Neored', 'Nombre', 'Cantidad a mano',
                        'Precio Tarifa', 'Impuestos del cliente']
    else:
        return False, f"Tipo de archivo desconocido: {file_type}"
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        return False, f"Columnas faltantes en archivo {file_type}: {', '.join(missing_cols)}"
    return True, "OK"

def extract_tax_percentage(tax_text: str) -> float:
    """
    Extrae el porcentaje de impuesto del texto.
    Ej: "IVA Ventas 21%" -> 0.21
    """
    if pd.isna(tax_text) or tax_text is None:
        return 0.0
    tax_text = str(tax_text).strip()
    match = re.search(r'(\d+(?:\.\d+)?)\s*%', tax_text)
    if match:
        return float(match.group(1)) / 100.0
    return 0.0