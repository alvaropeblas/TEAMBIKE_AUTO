import openpyxl
import mysql.connector
from mysql.connector import Error
import config  # Importar variables de entorno con credenciales de la base de datos
from datetime import datetime

# Función para crear la conexión a la base de datos
def crear_conexion():
    try:
        conn = mysql.connector.connect(
            host=config.DB_HOST,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            database=config.DB_NAME
        )
        if conn.is_connected():
            print("Conexión establecida con la base de datos MySQL.")
            return conn
    except Error as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

# Función para leer el archivo Excel
def leer_excel(ruta_archivo):
    wb = openpyxl.load_workbook(ruta_archivo)
    hoja = wb.active
    datos_extraidos = []

    for fila in hoja.iter_rows(min_row=2, values_only=True):
        # Extraer las columnas deseadas
        datos = {
            'Nombre': fila[0],
            'Ean13': fila[2],
            'Reference': fila[3],
            'Marca': fila[4],
            'Color': fila[5],
            'Categoria': fila[7],
            'Subcategoria': fila[6],
            'PVP': fila[9],
            'Descuento': fila[10],
            'Costo': fila[11],
            'Resumen': fila[16],
            'Keyword': fila[16],
            'Meta_Titulo': fila[17],
            'Meta_Descripcion': fila[18],
            'Imagen': fila[12]
        }
        datos_extraidos.append(datos)

    return datos_extraidos

# Función para insertar datos en la base de datos
def insertar_datos(conn, datos):
    try:
        for item in datos:
            cursor = conn.cursor()
            # Comprobar si el producto ya existe
            cursor.execute("""
            SELECT * FROM ps_product WHERE ean13 = %s OR reference = %s
            """, (item['Ean13'], item['Reference']))
            resultado = cursor.fetchall()  # Obtener todos los resultados

            if not resultado:  # Verificar si resultado está vacío
                # Obtener el id del fabricante
                cursor.execute("""
                SELECT id_manufacturer FROM ps_manufacturer WHERE name LIKE %s
                """, (item['Marca'],))
                fabricante_resultado = cursor.fetchone()  # Obtener solo un resultado
                # No es necesario hacer commit aquí para SELECT
                
                if fabricante_resultado:
                    id_manufacturer = fabricante_resultado[0]
                else:
                    print(f"Fabricante no encontrado: {item['Marca']}")
                    cursor.close()  # Cerrar el cursor antes de continuar
                    continue

                # Insertar el nuevo producto
                cursor.execute("""
                INSERT INTO ps_product (id_supplier, id_manufacturer, id_category_default, id_shop_default, id_tax_rules_group, ean13, price, wholesale_price, reference, date_add, date_upd, external_image_url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    11,  # ID del proveedor (ajusta según sea necesario)
                    id_manufacturer,
                    item['Categoria'],
                    1,
                    5,
                    item['Ean13'],
                    round(float(item['PVP'].replace(',', '.').strip()) / 1.21, 2),  # Precio sin IVA
                    item['Costo'],
                    item['Reference'],
                    datetime.now(),
                    datetime.now(),
                    item['Imagen']
                ))

                id_product = cursor.lastrowid

                # Insertar en ps_product_lang
                cursor.execute("""
                INSERT INTO ps_product_lang (id_product, id_shop, id_lang, description, meta_keywords, meta_title, meta_description, name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    id_product,
                    1,
                    1,
                    f"<h2>{item['Resumen']}</h2>",
                    item['Keyword'],
                    item['Meta_Titulo'],
                    item['Meta_Descripcion'],
                    item['Nombre']
                ))

                # Insertar en ps_category_product
                cursor.execute("""
                INSERT INTO ps_category_product (id_category, id_product)
                VALUES (%s, %s)
                """, (item['Subcategoria'], id_product))
                
                cursor.execute("""
                INSERT INTO ps_category_product (id_category, id_product)
                VALUES (%s, %s)
                """, (item['Categoria'], id_product))

                # Insertar en ps_product_shop
                cursor.execute("""
                INSERT INTO ps_product_shop (id_product, id_shop, id_category_default, id_tax_rules_group, price, wholesale_price, date_add, date_upd)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    id_product,
                    1,
                    item['Categoria'],
                    5,
                    round(float(item['PVP'].replace(',', '.').strip()) / 1.21, 2),  # Precio sin IVA
                    item['Costo'],
                    datetime.now(),
                    datetime.now()
                ))

                print(f"Producto insertado: {id_product}\n")
            else:
                print(f"Producto ya existe: {item['Ean13']} o {item['Reference']}\n")
        
            cursor.close()  # Cierra el cursor aquí para evitar conflictos

        conn.commit()  # Commit solo después de todas las inserciones

    except Error as e:
        print(f"Error al insertar datos: {e}")
        conn.rollback()  # Revertir cambios si hay un error

# Función principal
def main():
    ruta_archivo = 'teambike.xlsx'
    
    # Leer datos del Excel
    datos = leer_excel(ruta_archivo)
    
    # Crear conexión a la base de datos
    conn = crear_conexion()
    
    if conn:
        try:
            # Insertar los datos en la base de datos
            insertar_datos(conn, datos)
        finally:
            # Cerrar la conexión
            conn.close()

if __name__ == '__main__':
    main()
