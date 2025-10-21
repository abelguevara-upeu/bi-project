#!/usr/bin/env python3
"""
Script para crear la base de datos SQLite de la empresa molinera
Basado en el diagrama ER creado
Incluye estructura de tablas y datos poblados desde los CSV
"""

import sqlite3
import pandas as pd
import os
from datetime import datetime, date, timedelta
import random
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MolineraDatabaseCreator:
    def __init__(self, db_path='empresa_molinera.db'):
        self.db_path = db_path
        self.conn = None

    def connect(self):
        """Conectar a la base de datos SQLite"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.execute("PRAGMA foreign_keys = ON")
            logger.info(f"Conectado exitosamente a {self.db_path}")
            return True
        except Exception as e:
            logger.error(f"Error al conectar: {e}")
            return False

    def close(self):
        """Cerrar conexión"""
        if self.conn:
            self.conn.close()
            logger.info("Conexión cerrada")

    def create_tables(self):
        """Crear todas las tablas según el diagrama ER"""

        # Script SQL para crear todas las tablas
        create_tables_sql = """
        -- Eliminar tablas si existen (en orden correcto por dependencias)
        DROP TABLE IF EXISTS DISTRIBUCION;
        DROP TABLE IF EXISTS PRECIOS_PRODUCTO;
        DROP TABLE IF EXISTS VENTAS;
        DROP TABLE IF EXISTS PRODUCCION;
        DROP TABLE IF EXISTS INVENTARIOS;
        DROP TABLE IF EXISTS CLIENTES;
        DROP TABLE IF EXISTS ALMACENES;
        DROP TABLE IF EXISTS CANALES_DISTRIBUCION;
        DROP TABLE IF EXISTS MEDIOS_TRANSPORTE;
        DROP TABLE IF EXISTS INCOTERMS;
        DROP TABLE IF EXISTS PRODUCTOS;
        DROP TABLE IF EXISTS PAISES;

        -- Tablas maestras
        CREATE TABLE PAISES (
            id_pais INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo_pais TEXT UNIQUE NOT NULL,
            nombre_pais TEXT NOT NULL,
            region TEXT,
            tipo_cambio_usd REAL DEFAULT 1.0,
            activo BOOLEAN DEFAULT 1
        );

        CREATE TABLE PRODUCTOS (
            id_producto INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo_producto TEXT UNIQUE NOT NULL,
            nombre_producto TEXT NOT NULL,
            descripcion TEXT,
            tipo_harina TEXT,
            peso_kg REAL,
            unidad_empaque TEXT,
            precio_base REAL,
            fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
            activo BOOLEAN DEFAULT 1
        );

        CREATE TABLE ALMACENES (
            id_almacen INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo_almacen TEXT UNIQUE NOT NULL,
            nombre_almacen TEXT NOT NULL,
            id_pais INTEGER NOT NULL,
            direccion TEXT,
            capacidad_maxima_ton REAL,
            activo BOOLEAN DEFAULT 1,
            FOREIGN KEY (id_pais) REFERENCES PAISES(id_pais)
        );

        CREATE TABLE CLIENTES (
            id_cliente INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo_cliente TEXT UNIQUE NOT NULL,
            nombre_cliente TEXT NOT NULL,
            tipo_cliente TEXT CHECK(tipo_cliente IN ('Nacional', 'Exportación')),
            id_pais INTEGER NOT NULL,
            direccion TEXT,
            contacto TEXT,
            telefono TEXT,
            email TEXT,
            activo BOOLEAN DEFAULT 1,
            FOREIGN KEY (id_pais) REFERENCES PAISES(id_pais)
        );

        CREATE TABLE CANALES_DISTRIBUCION (
            id_canal INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo_canal TEXT NOT NULL,
            nombre_canal TEXT NOT NULL,
            descripcion TEXT,
            activo BOOLEAN DEFAULT 1
        );

        CREATE TABLE MEDIOS_TRANSPORTE (
            id_medio_transporte INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo_transporte TEXT NOT NULL,
            tipo_transporte TEXT CHECK(tipo_transporte IN ('Terrestre', 'Marítimo', 'Aéreo')),
            costo_base_km REAL DEFAULT 0,
            activo BOOLEAN DEFAULT 1
        );

        CREATE TABLE INCOTERMS (
            id_incoterm INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo_incoterm TEXT UNIQUE NOT NULL,
            descripcion TEXT,
            responsabilidades TEXT,
            activo BOOLEAN DEFAULT 1
        );

        -- Tablas operativas
        CREATE TABLE INVENTARIOS (
            id_inventario INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha_registro DATE NOT NULL,
            id_producto INTEGER NOT NULL,
            id_almacen INTEGER NOT NULL,
            stock_inicial_ton REAL DEFAULT 0,
            entradas_ton REAL DEFAULT 0,
            salidas_ton REAL DEFAULT 0,
            stock_final_ton REAL DEFAULT 0,
            stock_minimo_ton REAL DEFAULT 0,
            stock_maximo_ton REAL DEFAULT 0,
            costo_unitario REAL DEFAULT 0,
            valor_total REAL DEFAULT 0,
            estado_stock TEXT CHECK(estado_stock IN ('Óptimo', 'Bajo', 'Crítico', 'Exceso')),
            fecha_actualizacion DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (id_producto) REFERENCES PRODUCTOS(id_producto),
            FOREIGN KEY (id_almacen) REFERENCES ALMACENES(id_almacen)
        );

        CREATE TABLE PRODUCCION (
            id_produccion INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha_produccion DATE NOT NULL,
            id_producto INTEGER NOT NULL,
            id_almacen INTEGER NOT NULL,
            cantidad_producida_ton REAL DEFAULT 0,
            horas_produccion REAL DEFAULT 0,
            costo_materia_prima REAL DEFAULT 0,
            costo_mano_obra REAL DEFAULT 0,
            costo_indirecto REAL DEFAULT 0,
            costo_total REAL DEFAULT 0,
            turno TEXT CHECK(turno IN ('Mañana', 'Tarde', 'Noche')),
            estado_lote TEXT CHECK(estado_lote IN ('Aprobado', 'Rechazado', 'En_Proceso')),
            numero_lote TEXT UNIQUE,
            fecha_vencimiento DATETIME,
            FOREIGN KEY (id_producto) REFERENCES PRODUCTOS(id_producto),
            FOREIGN KEY (id_almacen) REFERENCES ALMACENES(id_almacen)
        );

        CREATE TABLE VENTAS (
            id_venta INTEGER PRIMARY KEY AUTOINCREMENT,
            nro_pedido TEXT UNIQUE NOT NULL,
            fecha_venta DATE NOT NULL,
            id_producto INTEGER NOT NULL,
            id_cliente INTEGER NOT NULL,
            id_incoterm INTEGER,
            id_medio_transporte INTEGER,
            precio_por_saco REAL NOT NULL,
            cantidad_sacos INTEGER NOT NULL,
            cantidad_toneladas REAL NOT NULL,
            total_venta REAL NOT NULL,
            moneda TEXT CHECK(moneda IN ('PEN', 'USD')) DEFAULT 'PEN',
            tipo_cambio REAL DEFAULT 1.0,
            estado_venta TEXT CHECK(estado_venta IN ('Pendiente', 'Confirmado', 'Enviado', 'Entregado', 'Cancelado')) DEFAULT 'Pendiente',
            fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
            fecha_actualizacion DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (id_producto) REFERENCES PRODUCTOS(id_producto),
            FOREIGN KEY (id_cliente) REFERENCES CLIENTES(id_cliente),
            FOREIGN KEY (id_incoterm) REFERENCES INCOTERMS(id_incoterm),
            FOREIGN KEY (id_medio_transporte) REFERENCES MEDIOS_TRANSPORTE(id_medio_transporte)
        );

        CREATE TABLE DISTRIBUCION (
            id_distribucion INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha_distribucion DATE NOT NULL,
            id_venta INTEGER NOT NULL,
            id_almacen_origen INTEGER NOT NULL,
            id_almacen_destino INTEGER,
            id_canal INTEGER,
            id_medio_transporte INTEGER,
            cantidad_distribuida_ton REAL DEFAULT 0,
            costo_transporte REAL DEFAULT 0,
            tiempo_entrega_dias INTEGER DEFAULT 0,
            estado_distribucion TEXT CHECK(estado_distribucion IN ('Programado', 'En_Transito', 'Entregado', 'Incidencia')) DEFAULT 'Programado',
            numero_guia TEXT,
            fecha_salida DATETIME,
            fecha_llegada_estimada DATETIME,
            fecha_llegada_real DATETIME,
            FOREIGN KEY (id_venta) REFERENCES VENTAS(id_venta),
            FOREIGN KEY (id_almacen_origen) REFERENCES ALMACENES(id_almacen),
            FOREIGN KEY (id_almacen_destino) REFERENCES ALMACENES(id_almacen),
            FOREIGN KEY (id_canal) REFERENCES CANALES_DISTRIBUCION(id_canal),
            FOREIGN KEY (id_medio_transporte) REFERENCES MEDIOS_TRANSPORTE(id_medio_transporte)
        );

        CREATE TABLE PRECIOS_PRODUCTO (
            id_precio INTEGER PRIMARY KEY AUTOINCREMENT,
            id_producto INTEGER NOT NULL,
            id_pais INTEGER NOT NULL,
            precio_venta REAL NOT NULL,
            moneda TEXT CHECK(moneda IN ('PEN', 'USD')) DEFAULT 'PEN',
            fecha_vigencia_inicio DATE NOT NULL,
            fecha_vigencia_fin DATE,
            tipo_cliente TEXT CHECK(tipo_cliente IN ('Nacional', 'Exportación')),
            activo BOOLEAN DEFAULT 1,
            FOREIGN KEY (id_producto) REFERENCES PRODUCTOS(id_producto),
            FOREIGN KEY (id_pais) REFERENCES PAISES(id_pais)
        );

        -- Índices para mejorar rendimiento
        CREATE INDEX idx_inventarios_fecha ON INVENTARIOS(fecha_registro);
        CREATE INDEX idx_inventarios_producto ON INVENTARIOS(id_producto);
        CREATE INDEX idx_ventas_fecha ON VENTAS(fecha_venta);
        CREATE INDEX idx_ventas_cliente ON VENTAS(id_cliente);
        CREATE INDEX idx_produccion_fecha ON PRODUCCION(fecha_produccion);
        CREATE INDEX idx_distribucion_fecha ON DISTRIBUCION(fecha_distribucion);
        """

        try:
            self.conn.executescript(create_tables_sql)
            self.conn.commit()
            logger.info("Tablas creadas exitosamente")
            return True
        except Exception as e:
            logger.error(f"Error creando tablas: {e}")
            return False

    def populate_master_data(self):
        """Poblar tablas maestras con datos base"""

        try:
            # Países
            paises_data = [
                ('PE', 'Perú', 'América del Sur', 1.0),
                ('CO', 'Colombia', 'América del Sur', 0.25),
                ('VE', 'Venezuela', 'América del Sur', 0.028),
                ('GT', 'Guatemala', 'América Central', 7.75),
                ('PA', 'Panamá', 'América Central', 1.0),
                ('SV', 'El Salvador', 'América Central', 1.0),
                ('CU', 'Cuba', 'Caribe', 24.0),
                ('CL', 'Chile', 'América del Sur', 900.0),
                ('US', 'Estados Unidos', 'América del Norte', 1.0)
            ]

            self.conn.executemany("""
                INSERT INTO PAISES (codigo_pais, nombre_pais, region, tipo_cambio_usd)
                VALUES (?, ?, ?, ?)
            """, paises_data)

            # Productos (extraer únicos de los CSV)
            productos_unicos = set()

            # Leer CSV de ventas para extraer productos
            if os.path.exists('dataset/DATA_VENTAS_HARINA_FINAL.csv'):
                df_ventas = pd.read_csv('dataset/DATA_VENTAS_HARINA_FINAL.csv')
                for producto in df_ventas['Producto'].unique():
                    productos_unicos.add(producto)

            # Leer CSV de inventario para extraer productos
            if os.path.exists('dataset/DATA_INVENTARIO HARINA FINAL.csv'):
                df_inventario = pd.read_csv('dataset/DATA_INVENTARIO HARINA FINAL.csv')
                for producto in df_inventario['Producto'].unique():
                    productos_unicos.add(producto)

            productos_data = []
            for i, producto in enumerate(productos_unicos, 1):
                codigo = f"PROD_{i:03d}"
                peso = 50 if '50 kg' in producto else 25 if '25 kg' in producto else 0
                tipo_harina = 'Galletera' if 'galletera' in producto.lower() else 'Integral' if 'integral' in producto.lower() else 'Panadera'
                productos_data.append((codigo, producto, f"Harina tipo {tipo_harina}", tipo_harina, peso, 'Sacos', 0.0))

            self.conn.executemany("""
                INSERT INTO PRODUCTOS (codigo_producto, nombre_producto, descripcion, tipo_harina, peso_kg, unidad_empaque, precio_base)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, productos_data)

            # Almacenes
            almacenes_data = [
                ('ALM_001', 'Almacén Lima', 1, 'Lima, Perú', 1000.0),  # id_pais = 1 (Perú)
                ('ALM_002', 'Almacén Arequipa', 1, 'Arequipa, Perú', 500.0),
                ('ALM_003', 'Almacén Bogotá', 2, 'Bogotá, Colombia', 300.0),  # id_pais = 2 (Colombia)
            ]

            self.conn.executemany("""
                INSERT INTO ALMACENES (codigo_almacen, nombre_almacen, id_pais, direccion, capacidad_maxima_ton)
                VALUES (?, ?, ?, ?, ?)
            """, almacenes_data)

            # Extraer clientes únicos de ventas CSV
            clientes_unicos = set()
            paises_clientes = {}

            if os.path.exists('dataset/DATA_VENTAS_HARINA_FINAL.csv'):
                df_ventas = pd.read_csv('dataset/DATA_VENTAS_HARINA_FINAL.csv')
                for _, row in df_ventas.iterrows():
                    cliente = row['Cliente']
                    pais = row['País']
                    tipo_cliente = row['Tipo_Cliente']
                    clientes_unicos.add(cliente)
                    paises_clientes[cliente] = (pais, tipo_cliente)

            # Mapeo de países a IDs
            pais_to_id = {
                'Perú': 1, 'Colombia': 2, 'Venezuela': 3, 'Guatemala': 4,
                'Panamá': 5, 'El Salvador': 6, 'Cuba': 7, 'Chile': 8, 'EEUU': 9
            }

            clientes_data = []
            for i, cliente in enumerate(clientes_unicos, 1):
                codigo = f"CLI_{i:03d}"
                pais, tipo_cliente = paises_clientes.get(cliente, ('Perú', 'Nacional'))
                id_pais = pais_to_id.get(pais, 1)
                clientes_data.append((codigo, cliente, tipo_cliente, id_pais, f"Dirección {cliente}", "Contacto", "123456789", f"contacto@{cliente.lower().replace(' ', '')}.com"))

            self.conn.executemany("""
                INSERT INTO CLIENTES (codigo_cliente, nombre_cliente, tipo_cliente, id_pais, direccion, contacto, telefono, email)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, clientes_data)

            # Medios de transporte
            transportes_data = [
                ('TERR_001', 'Terrestre', 0.5),
                ('MAR_001', 'Marítimo', 0.2),
                ('AER_001', 'Aéreo', 2.0)
            ]

            self.conn.executemany("""
                INSERT INTO MEDIOS_TRANSPORTE (codigo_transporte, tipo_transporte, costo_base_km)
                VALUES (?, ?, ?)
            """, transportes_data)

            # Incoterms
            incoterms_data = [
                ('FOB', 'Free On Board', 'Vendedor entrega mercancía en puerto de origen'),
                ('CIF', 'Cost, Insurance and Freight', 'Vendedor paga costo, seguro y flete'),
                ('DAP', 'Delivered At Place', 'Entregado en lugar acordado')
            ]

            self.conn.executemany("""
                INSERT INTO INCOTERMS (codigo_incoterm, descripcion, responsabilidades)
                VALUES (?, ?, ?)
            """, incoterms_data)

            # Canales de distribución
            canales_data = [
                ('RETAIL', 'Venta Retail', 'Venta directa a consumidor final'),
                ('WHOLESALE', 'Venta Mayorista', 'Venta a distribuidores'),
                ('EXPORT', 'Exportación', 'Venta internacional')
            ]

            self.conn.executemany("""
                INSERT INTO CANALES_DISTRIBUCION (codigo_canal, nombre_canal, descripcion)
                VALUES (?, ?, ?)
            """, canales_data)

            self.conn.commit()
            logger.info("Datos maestros poblados exitosamente")
            return True

        except Exception as e:
            logger.error(f"Error poblando datos maestros: {e}")
            return False

    def populate_operational_data(self):
        """Poblar tablas operativas desde los archivos CSV"""

        try:
            # Obtener mapeos de IDs
            productos_map = {}
            cursor = self.conn.cursor()
            cursor.execute("SELECT id_producto, nombre_producto FROM PRODUCTOS")
            for row in cursor.fetchall():
                productos_map[row[1]] = row[0]

            clientes_map = {}
            cursor.execute("SELECT id_cliente, nombre_cliente FROM CLIENTES")
            for row in cursor.fetchall():
                clientes_map[row[1]] = row[0]

            almacenes_map = {'Almacén Lima': 1}  # Por defecto

            # Poblar VENTAS desde CSV
            if os.path.exists('dataset/DATA_VENTAS_HARINA_FINAL.csv'):
                df_ventas = pd.read_csv('dataset/DATA_VENTAS_HARINA_FINAL.csv')

                ventas_data = []
                nro_pedidos_usados = set()
                contador_venta = 1

                for _, row in df_ventas.iterrows():
                    id_producto = productos_map.get(row['Producto'])
                    id_cliente = clientes_map.get(row['Cliente'])

                    # Mapear incoterm y transporte
                    incoterm_map = {'FOB': 1, 'CIF': 2, 'DAP': 3, '': None}
                    transporte_map = {'Terrestre': 1, 'Marítimo': 2, 'Aéreo': 3, '': None}

                    id_incoterm = incoterm_map.get(str(row.get('Incoterm', '')), None)
                    id_transporte = transporte_map.get(str(row.get('Medio_Transporte', '')), None)

                    # Generar número de pedido único
                    nro_pedido_original = str(row['Nro_Pedido'])
                    nro_pedido_unico = nro_pedido_original

                    # Si el pedido ya existe, crear uno único
                    if nro_pedido_unico in nro_pedidos_usados:
                        nro_pedido_unico = f"{nro_pedido_original}_{contador_venta:03d}"
                        while nro_pedido_unico in nro_pedidos_usados:
                            contador_venta += 1
                            nro_pedido_unico = f"{nro_pedido_original}_{contador_venta:03d}"

                    nro_pedidos_usados.add(nro_pedido_unico)

                    if id_producto and id_cliente:
                        # Obtener peso del saco del producto
                        cursor.execute("SELECT peso_kg FROM PRODUCTOS WHERE id_producto = ?", (id_producto,))
                        result = cursor.fetchone()
                        peso_saco_kg = result[0] if result and result[0] > 0 else 50

                        # Calcular cantidades correctas
                        cantidad_toneladas = row['Cantidad_toneladas']
                        cantidad_sacos = int((cantidad_toneladas * 1000) / peso_saco_kg) if peso_saco_kg > 0 else 0
                        precio_por_saco = row['Precio_Unitario']  # En CSV es precio por saco

                        # Recalcular total correcto
                        total_correcto = precio_por_saco * cantidad_sacos

                        ventas_data.append((
                            nro_pedido_unico,
                            row['Fecha_Venta'],
                            id_producto,
                            id_cliente,
                            id_incoterm,
                            id_transporte,
                            precio_por_saco,
                            cantidad_sacos,
                            cantidad_toneladas,
                            total_correcto,
                            row.get('Moneda', 'PEN'),
                            1.0,  # tipo_cambio por defecto
                            'Entregado'
                        ))

                self.conn.executemany("""
                    INSERT INTO VENTAS (nro_pedido, fecha_venta, id_producto, id_cliente, id_incoterm,
                                      id_medio_transporte, precio_por_saco, cantidad_sacos, cantidad_toneladas,
                                      total_venta, moneda, tipo_cambio, estado_venta)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, ventas_data)

            # Poblar INVENTARIOS desde CSV
            if os.path.exists('dataset/DATA_INVENTARIO HARINA FINAL.csv'):
                df_inventario = pd.read_csv('dataset/DATA_INVENTARIO HARINA FINAL.csv')

                inventarios_data = []
                for _, row in df_inventario.iterrows():
                    id_producto = productos_map.get(row['Producto'])
                    id_almacen = 1  # Por defecto Almacén Lima

                    if id_producto:
                        inventarios_data.append((
                            row['Fecha_Registro'],
                            id_producto,
                            id_almacen,
                            row['Stock_Inicial_ton'],
                            row['Entradas_ton'],
                            row['Salidas_ton'],
                            row['Stock_Final_ton'],
                            row['Stock_Mínimo_ton'],
                            row['Stock_Máximo_ton'],
                            row['Costo_Unitario_Soles'],
                            row['Valor_Total_Soles'],
                            row['Estado_Stock']
                        ))

                self.conn.executemany("""
                    INSERT INTO INVENTARIOS (fecha_registro, id_producto, id_almacen, stock_inicial_ton,
                                           entradas_ton, salidas_ton, stock_final_ton, stock_minimo_ton,
                                           stock_maximo_ton, costo_unitario, valor_total, estado_stock)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, inventarios_data)

            # Poblar PRODUCCION desde CSV
            if os.path.exists('dataset/DATA_PRODUCCIÓN HARINA.csv'):
                df_produccion = pd.read_csv('dataset/DATA_PRODUCCIÓN HARINA.csv')

                produccion_data = []
                for _, row in df_produccion.iterrows():
                    id_producto = productos_map.get(row['Producto'])
                    id_almacen = 1  # Por defecto Almacén Lima

                    # Mapear estado del lote
                    estado_map = {'Completado': 'Aprobado', 'En proceso': 'En_Proceso', 'Rechazado': 'Rechazado'}
                    estado_lote = estado_map.get(row.get('Estado_Lote', ''), 'En_Proceso')

                    # Calcular costos (simplificados)
                    cantidad_kg = row.get('Cantidad_Producida_kg', 0)
                    cantidad_ton = cantidad_kg / 1000 if cantidad_kg else 0
                    costo_total = row.get('Costo_Producción_Soles', 0)

                    # Distribución de costos (estimada)
                    costo_materia_prima = costo_total * 0.6
                    costo_mano_obra = costo_total * 0.25
                    costo_indirecto = costo_total * 0.15

                    # Generar fecha de vencimiento coherente
                    fecha_prod = datetime.strptime(row['Fecha_Producción'], '%Y-%m-%d')
                    # Harina tiene vencimiento entre 6-12 meses según tipo
                    if 'integral' in row['Producto'].lower():
                        meses_venc = random.randint(6, 8)  # Integral vence más rápido
                    else:
                        meses_venc = random.randint(10, 12)  # Refinada dura más
                    fecha_vencimiento = fecha_prod + timedelta(days=meses_venc * 30)

                    if id_producto:
                        produccion_data.append((
                            row['Fecha_Producción'],
                            id_producto,
                            id_almacen,
                            cantidad_ton,
                            row.get('Tiempo_Producción_horas', 0),
                            costo_materia_prima,
                            costo_mano_obra,
                            costo_indirecto,
                            costo_total,
                            row.get('Turno', 'Mañana'),
                            estado_lote,
                            row['Lote_Producción'],
                            fecha_vencimiento.strftime('%Y-%m-%d %H:%M:%S')
                        ))

                self.conn.executemany("""
                    INSERT INTO PRODUCCION (fecha_produccion, id_producto, id_almacen, cantidad_producida_ton,
                                          horas_produccion, costo_materia_prima, costo_mano_obra, costo_indirecto,
                                          costo_total, turno, estado_lote, numero_lote, fecha_vencimiento)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, produccion_data)

            # Poblar PRECIOS_PRODUCTO desde CSV
            if os.path.exists('dataset/PRECIOS POR PRODUCTO.csv'):
                df_precios = pd.read_csv('dataset/PRECIOS POR PRODUCTO.csv')

                precios_data = []
                for _, row in df_precios.iterrows():
                    id_producto = productos_map.get(row['Producto'])

                    if id_producto:
                        # Crear precios para diferentes países y tipos de cliente
                        precio_base = row['Precio_saco_50kg']

                        # Precio para Perú (Nacional)
                        precios_data.append((
                            id_producto, 1, precio_base, 'PEN',
                            '2024-01-01', '2024-12-31', 'Nacional'
                        ))

                        # Precio para exportación (USD, con markup)
                        precio_usd = precio_base * 0.27  # Conversión aproximada PEN a USD
                        for id_pais in [2, 3, 4, 5, 6, 7, 8, 9]:  # Otros países
                            precios_data.append((
                                id_producto, id_pais, precio_usd * 1.15, 'USD',
                                '2024-01-01', '2024-12-31', 'Exportación'
                            ))

                self.conn.executemany("""
                    INSERT INTO PRECIOS_PRODUCTO (id_producto, id_pais, precio_venta, moneda,
                                                fecha_vigencia_inicio, fecha_vigencia_fin, tipo_cliente)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, precios_data)

            # Poblar DISTRIBUCION desde CSV
            if os.path.exists('dataset/DATA_DISTRIBUCION_HARINA_FINAL.csv'):
                df_distribucion = pd.read_csv('dataset/DATA_DISTRIBUCION_HARINA_FINAL.csv')

                # Obtener mapeo de ventas por pedido
                ventas_map = {}
                cursor.execute("SELECT id_venta, nro_pedido FROM VENTAS")
                for row in cursor.fetchall():
                    # Extraer número base del pedido (sin sufijos _001, etc)
                    nro_base = row[1].split('_')[0]
                    ventas_map[nro_base] = row[0]

                distribucion_data = []
                contador_dist = 1

                for _, row in df_distribucion.iterrows():
                    pedido_id = str(row.get('Pedido_ID', ''))
                    id_venta = ventas_map.get(pedido_id)

                    if id_venta:
                        # Mapear canal de distribución
                        canal_map = {'Tienda física': 1, 'Online': 2, 'Distribuidor': 3}
                        id_canal = canal_map.get(row.get('Canal_Venta', ''), 1)

                        # Mapear medio de transporte
                        transporte_map = {'Terrestre': 1, 'Marítimo': 2, 'Aéreo': 3}
                        id_transporte = transporte_map.get(row.get('Medio_Transporte', ''), 1)

                        # Calcular cantidad en toneladas
                        cantidad_sacos = row.get('Cantidad_total_sacos', 0)
                        peso_saco = 50 if '50 kg' in str(row.get('Producto', '')) else 25
                        cantidad_ton = (cantidad_sacos * peso_saco) / 1000

                        # Estado de distribución
                        estado_dist = 'Entregado' if row.get('Devolución', 'No') == 'No' else 'Incidencia'

                        # Determinar almacén destino basado en la ubicación del cliente
                        pais_cliente = row.get('País', 'Perú')
                        ciudad_cliente = row.get('Ciudad', '')

                        if pais_cliente == 'Perú':
                            # Para Perú, usar almacenes según la región
                            if ciudad_cliente in ['Lima', 'Callao', 'Chiclayo', 'Trujillo', 'Piura']:
                                id_almacen_destino = 1  # Almacén Lima
                            elif ciudad_cliente in ['Arequipa', 'Cusco', 'Tacna']:
                                id_almacen_destino = 2  # Almacén Arequipa
                            else:
                                id_almacen_destino = 1  # Default Lima
                        elif pais_cliente == 'Colombia':
                            id_almacen_destino = 3  # Almacén Bogotá
                        else:
                            # Para otros países, exportación desde Lima
                            id_almacen_destino = None  # Sin almacén destino para exportación

                        distribucion_data.append((
                            row['Fecha_Pedido'],
                            id_venta,
                            1,  # almacen_origen (Lima)
                            id_almacen_destino,
                            id_canal,
                            id_transporte,
                            cantidad_ton,
                            row.get('Costo_Envío', 0),
                            7,  # tiempo_entrega_dias estimado
                            estado_dist,
                            f"GUIA-{contador_dist:06d}",
                            row['Fecha_Pedido'],  # fecha_salida
                            row['Fecha_Pedido'],  # fecha_llegada_estimada
                            row['Fecha_Pedido']   # fecha_llegada_real
                        ))
                        contador_dist += 1

                self.conn.executemany("""
                    INSERT INTO DISTRIBUCION (fecha_distribucion, id_venta, id_almacen_origen, id_almacen_destino,
                                            id_canal, id_medio_transporte, cantidad_distribuida_ton, costo_transporte,
                                            tiempo_entrega_dias, estado_distribucion, numero_guia, fecha_salida,
                                            fecha_llegada_estimada, fecha_llegada_real)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, distribucion_data)

            self.conn.commit()
            logger.info("Datos operacionales poblados exitosamente")
            return True

        except Exception as e:
            logger.error(f"Error poblando datos operacionales: {e}")
            return False

    def create_views(self):
        """Crear vistas útiles para análisis"""

        views_sql = """
        -- Vista resumen de ventas por producto
        CREATE VIEW IF NOT EXISTS v_ventas_por_producto AS
        SELECT
            p.nombre_producto,
            COUNT(*) as total_ventas,
            SUM(v.cantidad_sacos) as total_sacos,
            SUM(v.cantidad_toneladas) as total_toneladas,
            SUM(v.total_venta) as total_ingresos,
            AVG(v.precio_por_saco) as precio_promedio_saco
        FROM VENTAS v
        JOIN PRODUCTOS p ON v.id_producto = p.id_producto
        GROUP BY p.id_producto, p.nombre_producto;

        -- Vista resumen de inventario actual
        CREATE VIEW IF NOT EXISTS v_inventario_actual AS
        SELECT
            p.nombre_producto,
            a.nombre_almacen,
            i.stock_final_ton,
            i.estado_stock,
            i.valor_total,
            i.fecha_registro
        FROM INVENTARIOS i
        JOIN PRODUCTOS p ON i.id_producto = p.id_producto
        JOIN ALMACENES a ON i.id_almacen = a.id_almacen
        WHERE i.fecha_registro = (
            SELECT MAX(fecha_registro)
            FROM INVENTARIOS i2
            WHERE i2.id_producto = i.id_producto
            AND i2.id_almacen = i.id_almacen
        );

        -- Vista ventas por cliente
        CREATE VIEW IF NOT EXISTS v_ventas_por_cliente AS
        SELECT
            c.nombre_cliente,
            c.tipo_cliente,
            pa.nombre_pais,
            COUNT(*) as total_pedidos,
            SUM(v.cantidad_sacos) as total_sacos,
            SUM(v.cantidad_toneladas) as total_toneladas,
            SUM(v.total_venta) as total_facturado
        FROM VENTAS v
        JOIN CLIENTES c ON v.id_cliente = c.id_cliente
        JOIN PAISES pa ON c.id_pais = pa.id_pais
        GROUP BY c.id_cliente, c.nombre_cliente, c.tipo_cliente, pa.nombre_pais;
        """

        try:
            self.conn.executescript(views_sql)
            self.conn.commit()
            logger.info("Vistas creadas exitosamente")
            return True
        except Exception as e:
            logger.error(f"Error creando vistas: {e}")
            return False

    def generate_summary_report(self):
        """Generar reporte resumen de la base de datos"""

        try:
            cursor = self.conn.cursor()

            print("\n" + "="*60)
            print("RESUMEN DE BASE DE DATOS - EMPRESA MOLINERA")
            print("="*60)

            # Contar registros por tabla
            tables = ['PAISES', 'PRODUCTOS', 'ALMACENES', 'CLIENTES', 'VENTAS', 'INVENTARIOS', 'PRODUCCION', 'DISTRIBUCION', 'PRECIOS_PRODUCTO', 'CANALES_DISTRIBUCION', 'MEDIOS_TRANSPORTE', 'INCOTERMS']

            print("\n📊 REGISTROS POR TABLA:")
            print("-" * 40)
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"{table:25} {count:>8} registros")

            # Resumen de ventas
            cursor.execute("SELECT COUNT(*), SUM(total_venta), SUM(cantidad_toneladas) FROM VENTAS")
            total_ventas, total_ingresos, total_toneladas = cursor.fetchone()

            print(f"\n💰 RESUMEN DE VENTAS:")
            print("-" * 40)
            print(f"Total pedidos:           {total_ventas:>8}")
            print(f"Total ingresos:         ${total_ingresos:>8,.2f}")
            print(f"Total toneladas:        {total_toneladas:>8,.1f}")

            # Top productos
            cursor.execute("""
                SELECT p.nombre_producto, COUNT(*) as ventas
                FROM VENTAS v
                JOIN PRODUCTOS p ON v.id_producto = p.id_producto
                GROUP BY p.nombre_producto
                ORDER BY ventas DESC
                LIMIT 3
            """)

            print(f"\n🏆 TOP 3 PRODUCTOS MÁS VENDIDOS:")
            print("-" * 40)
            for i, (producto, ventas) in enumerate(cursor.fetchall(), 1):
                print(f"{i}. {producto[:35]:<35} {ventas:>3} ventas")

            print("\n✅ Base de datos creada exitosamente!")
            print(f"📁 Archivo: {self.db_path}")
            print("="*60)

        except Exception as e:
            logger.error(f"Error generando reporte: {e}")

def main():
    """Función principal"""

    print("🏭 CREADOR DE BASE DE DATOS - EMPRESA MOLINERA")
    print("="*50)

    # Crear instancia del creador de BD
    db_creator = MolineraDatabaseCreator()

    # Conectar a la BD
    if not db_creator.connect():
        print("❌ Error: No se pudo conectar a la base de datos")
        return

    try:
        # Crear tablas
        print("\n1️⃣  Creando estructura de tablas...")
        if not db_creator.create_tables():
            print("❌ Error creando tablas")
            return

        # Poblar datos maestros
        print("2️⃣  Poblando datos maestros...")
        if not db_creator.populate_master_data():
            print("❌ Error poblando datos maestros")
            return

        # Poblar datos operacionales
        print("3️⃣  Poblando datos operacionales desde CSV...")
        if not db_creator.populate_operational_data():
            print("❌ Error poblando datos operacionales")
            return

        # Crear vistas
        print("4️⃣  Creando vistas de análisis...")
        if not db_creator.create_views():
            print("❌ Error creando vistas")
            return

        # Generar reporte
        print("5️⃣  Generando reporte resumen...")
        db_creator.generate_summary_report()

    except Exception as e:
        logger.error(f"Error en proceso principal: {e}")

    finally:
        # Cerrar conexión
        db_creator.close()

if __name__ == "__main__":
    main()
