#!/usr/bin/env python3
"""
CREADOR DE DATAMARTS - EMPRESA MOLINERA
Implementación de datamarts dimensionales separados para Business Intelligence
Cada datamart tiene su propia base de datos SQLite
"""

import sqlite3
import pandas as pd
import logging
from datetime import datetime, timedelta, date
import random
from typing import Dict, List, Optional
import sys
import os

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('datamarts_creation.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class DatamartCreator:
    """Clase principal para crear los datamarts dimensionales separados"""

    def __init__(self, source_db_path: str = "empresa_molinera.db"):
        self.source_db_path = source_db_path
        self.source_conn = None
        self.datamart_connections = {}
        self.datamart_paths = {
            'ventas': 'datamart_ventas.db',
            'inventarios': 'datamart_inventarios.db',
            'distribucion': 'datamart_distribucion.db',
            'produccion': 'datamart_produccion.db'
        }

    def connect_databases(self):
        """Conectar a la base de datos origen y crear conexiones para cada datamart"""
        try:
            # Conectar a BD origen
            self.source_conn = sqlite3.connect(self.source_db_path)
            logger.info(f"✅ Conectado a BD origen: {self.source_db_path}")

            # Crear conexiones para cada datamart
            for datamart_name, db_path in self.datamart_paths.items():
                # Eliminar archivo existente
                if os.path.exists(db_path):
                    os.remove(db_path)
                    logger.info(f"🗑️  Eliminado archivo previo: {db_path}")

                self.datamart_connections[datamart_name] = sqlite3.connect(db_path)
                logger.info(f"✅ Creado datamart {datamart_name}: {db_path}")

            return True
        except Exception as e:
            logger.error(f"❌ Error conectando bases de datos: {e}")
            return False

    def close_connections(self):
        """Cerrar todas las conexiones a las bases de datos"""
        if self.source_conn:
            self.source_conn.close()

        for datamart_name, conn in self.datamart_connections.items():
            if conn:
                conn.close()
                logger.info(f"🔒 Conexión cerrada para datamart {datamart_name}")

        logger.info("🔒 Todas las conexiones cerradas")

    def create_dimension_tiempo(self):
        """Crear dimensión tiempo común para todos los datamarts"""
        logger.info("🕒 Creando dimensión TIEMPO en todos los datamarts...")

        sql_dim_tiempo = """
        CREATE TABLE IF NOT EXISTS DIM_TIEMPO (
            id_tiempo INTEGER PRIMARY KEY,
            fecha_completa DATE NOT NULL,
            año INTEGER NOT NULL,
            mes INTEGER NOT NULL,
            dia INTEGER NOT NULL,
            trimestre INTEGER NOT NULL,
            nombre_mes TEXT NOT NULL,
            nombre_dia_semana TEXT NOT NULL,
            numero_semana INTEGER NOT NULL,
            periodo_fiscal TEXT NOT NULL,
            es_fin_semana BOOLEAN NOT NULL,
            es_feriado BOOLEAN NOT NULL,
            turno_trabajo INTEGER DEFAULT 1,
            periodo_estacional TEXT
        )
        """

        # Crear tabla en cada datamart
        for datamart_name, conn in self.datamart_connections.items():
            conn.execute("DROP TABLE IF EXISTS DIM_TIEMPO")
            conn.execute(sql_dim_tiempo)
            logger.info(f"📅 Tabla DIM_TIEMPO creada en {datamart_name}")

        # Generar datos para la dimensión tiempo (últimos 2 años + próximo año)
        start_date = date(2023, 1, 1)
        end_date = date(2026, 12, 31)

        tiempo_data = []
        current_date = start_date
        id_tiempo = 1

        # Lista de feriados peruanos (básicos)
        feriados_peru = [
            (1, 1),   # Año Nuevo
            (7, 28),  # Fiestas Patrias
            (7, 29),  # Fiestas Patrias
            (12, 25), # Navidad
            (5, 1),   # Día del Trabajador
        ]

        while current_date <= end_date:
            # Calcular trimestre
            trimestre = ((current_date.month - 1) // 3) + 1

            # Nombres de meses
            nombres_meses = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
                           'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']

            # Nombres de días
            nombres_dias = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']

            # Verificar si es feriado
            es_feriado = (current_date.month, current_date.day) in feriados_peru

            # Verificar si es fin de semana (sábado=5, domingo=6)
            es_fin_semana = current_date.weekday() >= 5

            # Determinar período estacional (para producción agrícola)
            if current_date.month in [12, 1, 2]:
                periodo_estacional = "Verano"
            elif current_date.month in [3, 4, 5]:
                periodo_estacional = "Otoño"
            elif current_date.month in [6, 7, 8]:
                periodo_estacional = "Invierno"
            else:
                periodo_estacional = "Primavera"

            # Turno de trabajo (1=Mañana, 2=Tarde, 3=Noche)
            turno = random.choice([1, 2, 3])

            tiempo_data.append((
                id_tiempo,
                current_date,
                current_date.year,
                current_date.month,
                current_date.day,
                trimestre,
                nombres_meses[current_date.month - 1],
                nombres_dias[current_date.weekday()],
                current_date.isocalendar()[1],  # Número de semana
                f"FY{current_date.year}",  # Período fiscal
                es_fin_semana,
                es_feriado,
                turno,
                periodo_estacional
            ))

            current_date += timedelta(days=1)
            id_tiempo += 1

        # Insertar datos en cada datamart
        insert_sql = """
        INSERT INTO DIM_TIEMPO (
            id_tiempo, fecha_completa, año, mes, dia, trimestre, nombre_mes,
            nombre_dia_semana, numero_semana, periodo_fiscal, es_fin_semana,
            es_feriado, turno_trabajo, periodo_estacional
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        for datamart_name, conn in self.datamart_connections.items():
            conn.executemany(insert_sql, tiempo_data)
            conn.commit()
            logger.info(f"📊 Dimensión TIEMPO poblada en {datamart_name} con {len(tiempo_data)} registros")

    def create_datamart_ventas(self):
        """Crear datamart de ventas completo"""
        logger.info("🛒 Creando Datamart de VENTAS...")

        conn_ventas = self.datamart_connections['ventas']

        # Crear dimensiones específicas
        logger.info("📋 Creando dimensiones para Ventas...")

        # Dimensión Producto
        conn_ventas.execute("DROP TABLE IF EXISTS DIM_PRODUCTO")
        conn_ventas.execute("""
        CREATE TABLE DIM_PRODUCTO (
            id_producto INTEGER PRIMARY KEY,
            nombre_producto TEXT NOT NULL,
            tipo_harina TEXT,
            peso_kg REAL,
            categoria TEXT,
            subcategoria TEXT,
            marca TEXT,
            es_premium BOOLEAN,
            unidad_medida TEXT
        )
        """)

        # Dimensión Cliente
        conn_ventas.execute("DROP TABLE IF EXISTS DIM_CLIENTE")
        conn_ventas.execute("""
        CREATE TABLE DIM_CLIENTE (
            id_cliente INTEGER PRIMARY KEY,
            nombre_cliente TEXT NOT NULL,
            tipo_cliente TEXT,
            segmento TEXT,
            tamaño_empresa TEXT,
            id_pais INTEGER,
            nombre_pais TEXT,
            region TEXT,
            fecha_registro DATE,
            estado_cliente TEXT
        )
        """)

        # Dimensión Geografía
        conn_ventas.execute("DROP TABLE IF EXISTS DIM_GEOGRAFIA")
        conn_ventas.execute("""
        CREATE TABLE DIM_GEOGRAFIA (
            id_geografia INTEGER PRIMARY KEY,
            pais TEXT NOT NULL,
            region TEXT,
            continente TEXT,
            zona_comercial TEXT,
            codigo_iso TEXT,
            es_mercado_principal BOOLEAN
        )
        """)

        # Dimensión Canal
        conn_ventas.execute("DROP TABLE IF EXISTS DIM_CANAL")
        conn_ventas.execute("""
        CREATE TABLE DIM_CANAL (
            id_canal INTEGER PRIMARY KEY,
            canal_distribucion TEXT NOT NULL,
            tipo_canal TEXT,
            descripcion TEXT,
            comision_porcentaje REAL,
            es_activo BOOLEAN
        )
        """)

        # Dimensión Transporte
        conn_ventas.execute("DROP TABLE IF EXISTS DIM_TRANSPORTE")
        conn_ventas.execute("""
        CREATE TABLE DIM_TRANSPORTE (
            id_transporte INTEGER PRIMARY KEY,
            medio_transporte TEXT NOT NULL,
            tipo_transporte TEXT,
            costo_promedio_km REAL,
            capacidad_ton REAL,
            es_internacional BOOLEAN
        )
        """)

        # Tabla de Hechos Ventas
        conn_ventas.execute("DROP TABLE IF EXISTS FACT_VENTAS")
        conn_ventas.execute("""
        CREATE TABLE FACT_VENTAS (
            id_venta INTEGER PRIMARY KEY,
            id_tiempo INTEGER,
            id_producto INTEGER,
            id_cliente INTEGER,
            id_geografia INTEGER,
            id_canal INTEGER,
            id_transporte INTEGER,
            nro_pedido TEXT,
            cantidad_sacos INTEGER,
            cantidad_toneladas REAL,
            precio_por_saco REAL,
            total_venta REAL,
            costo_producto REAL,
            margen_bruto REAL,
            descuento_aplicado REAL,
            moneda TEXT,
            tipo_cambio REAL,
            estado_venta TEXT,
            fecha_entrega DATE,
            dias_credito INTEGER,
            comision_venta REAL,
            FOREIGN KEY (id_tiempo) REFERENCES DIM_TIEMPO(id_tiempo),
            FOREIGN KEY (id_producto) REFERENCES DIM_PRODUCTO(id_producto),
            FOREIGN KEY (id_cliente) REFERENCES DIM_CLIENTE(id_cliente),
            FOREIGN KEY (id_geografia) REFERENCES DIM_GEOGRAFIA(id_geografia),
            FOREIGN KEY (id_canal) REFERENCES DIM_CANAL(id_canal),
            FOREIGN KEY (id_transporte) REFERENCES DIM_TRANSPORTE(id_transporte)
        )
        """)

        # Popular dimensiones
        self.populate_ventas_dimensions(conn_ventas)

        # Popular tabla de hechos
        self.populate_ventas_facts(conn_ventas)

        conn_ventas.commit()
        logger.info("✅ Datamart de VENTAS creado exitosamente")

    def populate_ventas_dimensions(self, conn_ventas):
        """Popular las dimensiones del datamart de ventas"""
        logger.info("📊 Poblando dimensiones de Ventas...")

        # Popular DIM_PRODUCTO desde la BD origen
        productos_df = pd.read_sql_query("""
            SELECT id_producto, nombre_producto, peso_kg
            FROM PRODUCTOS
        """, self.source_conn)

        for _, row in productos_df.iterrows():
            # Extraer información del nombre del producto
            nombre = row['nombre_producto']

            if "panadera" in nombre.lower():
                tipo_harina = "Panadera"
                categoria = "Panadería"
            elif "pastelera" in nombre.lower():
                tipo_harina = "Pastelera"
                categoria = "Pastelería"
            elif "galletera" in nombre.lower():
                tipo_harina = "Galletera"
                categoria = "Galletería"
            else:
                tipo_harina = "Especial"
                categoria = "Especial"

            es_premium = "premium" in nombre.lower()
            peso_kg = row['peso_kg']

            conn_ventas.execute("""
                INSERT INTO DIM_PRODUCTO (
                    id_producto, nombre_producto, tipo_harina, peso_kg,
                    categoria, subcategoria, marca, es_premium, unidad_medida
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row['id_producto'], nombre, tipo_harina, peso_kg,
                categoria, "Estándar", "Doña Angélica", es_premium, "Sacos"
            ))

        # Popular DIM_CLIENTE desde la BD origen
        clientes_df = pd.read_sql_query("""
            SELECT c.id_cliente, c.nombre_cliente, c.tipo_cliente,
                   c.id_pais, p.nombre_pais
            FROM CLIENTES c
            JOIN PAISES p ON c.id_pais = p.id_pais
        """, self.source_conn)

        for _, row in clientes_df.iterrows():
            # Determinar segmento basado en tipo de cliente
            tipo = row['tipo_cliente']
            if tipo == "Distribuidor Mayorista":
                segmento = "Mayorista"
                tamaño = "Grande"
            elif tipo == "Supermercado":
                segmento = "Retail"
                tamaño = "Grande"
            elif tipo.startswith("Panadería") or tipo.startswith("Pastelería"):
                segmento = "Artesanal"
                tamaño = "Pequeño"
            else:
                segmento = "Otros"
                tamaño = "Mediano"

            conn_ventas.execute("""
                INSERT INTO DIM_CLIENTE (
                    id_cliente, nombre_cliente, tipo_cliente, segmento,
                    tamaño_empresa, id_pais, nombre_pais, region,
                    fecha_registro, estado_cliente
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row['id_cliente'], row['nombre_cliente'], tipo, segmento,
                tamaño, row['id_pais'], row['nombre_pais'], "América",
                date(2023, 1, 1), "Activo"
            ))

        # Popular DIM_GEOGRAFIA desde la BD origen
        paises_df = pd.read_sql_query("""
            SELECT id_pais, nombre_pais FROM PAISES
        """, self.source_conn)

        for _, row in paises_df.iterrows():
            pais = row['nombre_pais']

            # Determinar continente y zona comercial
            if pais in ["Perú", "Colombia", "Ecuador", "Bolivia", "Chile", "Argentina", "Brasil", "Venezuela"]:
                continente = "América del Sur"
                zona_comercial = "Pacífico Sur"
                es_principal = pais == "Perú"
            elif pais == "Estados Unidos":
                continente = "América del Norte"
                zona_comercial = "Norteamérica"
                es_principal = False
            else:
                continente = "Otros"
                zona_comercial = "Internacional"
                es_principal = False

            conn_ventas.execute("""
                INSERT INTO DIM_GEOGRAFIA (
                    id_geografia, pais, region, continente,
                    zona_comercial, codigo_iso, es_mercado_principal
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                row['id_pais'], pais, continente, continente,
                zona_comercial, pais[:2].upper(), es_principal
            ))

        # Popular DIM_CANAL desde la BD origen
        canales_df = pd.read_sql_query("""
            SELECT id_canal, nombre_canal FROM CANALES_DISTRIBUCION
        """, self.source_conn)

        for _, row in canales_df.iterrows():
            canal = row['nombre_canal']

            if "directo" in canal.lower():
                tipo_canal = "Directo"
                comision = 0.0
            elif "distribuidor" in canal.lower():
                tipo_canal = "Indirecto"
                comision = 8.5
            else:
                tipo_canal = "Online"
                comision = 5.0

            conn_ventas.execute("""
                INSERT INTO DIM_CANAL (
                    id_canal, canal_distribucion, tipo_canal,
                    descripcion, comision_porcentaje, es_activo
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (
                row['id_canal'], canal, tipo_canal,
                f"Canal {tipo_canal}", comision, True
            ))

        # Popular DIM_TRANSPORTE desde la BD origen
        transportes_df = pd.read_sql_query("""
            SELECT id_medio_transporte as id_transporte, tipo_transporte
            FROM MEDIOS_TRANSPORTE
        """, self.source_conn)

        for _, row in transportes_df.iterrows():
            transporte = row['tipo_transporte']

            if "terrestre" in transporte.lower():
                tipo_transporte = "Terrestre"
                costo_km = 2.5
                capacidad = 25.0
                internacional = False
            elif "marítimo" in transporte.lower():
                tipo_transporte = "Marítimo"
                costo_km = 1.2
                capacidad = 500.0
                internacional = True
            else:
                tipo_transporte = "Aéreo"
                costo_km = 8.0
                capacidad = 10.0
                internacional = True

            conn_ventas.execute("""
                INSERT INTO DIM_TRANSPORTE (
                    id_transporte, medio_transporte, tipo_transporte,
                    costo_promedio_km, capacidad_ton, es_internacional
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (
                row['id_transporte'], transporte, tipo_transporte,
                costo_km, capacidad, internacional
            ))

        logger.info("✅ Dimensiones de Ventas pobladas exitosamente")

    def populate_ventas_facts(self, conn_ventas):
        """Popular la tabla de hechos de ventas"""
        logger.info("🎯 Poblando tabla de hechos VENTAS...")

        ventas_df = pd.read_sql_query("""
            SELECT v.*, p.peso_kg, c.id_pais
            FROM VENTAS v
            JOIN PRODUCTOS p ON v.id_producto = p.id_producto
            JOIN CLIENTES c ON v.id_cliente = c.id_cliente
        """, self.source_conn)

        for index, row in ventas_df.iterrows():
            # Obtener id_tiempo basado en fecha_venta
            fecha_venta = pd.to_datetime(row['fecha_venta']).date()

            cursor = conn_ventas.execute("""
                SELECT id_tiempo FROM DIM_TIEMPO WHERE fecha_completa = ?
            """, (fecha_venta,))

            tiempo_result = cursor.fetchone()
            id_tiempo = tiempo_result[0] if tiempo_result else 1

            # Usar id_pais como id_geografia
            id_geografia = row['id_pais']

            # Asignar canal aleatorio (1-3)
            id_canal = random.randint(1, 3)

            # Calcular métricas adicionales
            costo_producto = row['precio_por_saco'] * 0.65  # 65% del precio es costo
            margen_bruto = row['total_venta'] - (costo_producto * row['cantidad_sacos'])
            descuento_aplicado = row['total_venta'] * random.uniform(0, 0.1)  # 0-10% descuento
            comision_venta = row['total_venta'] * 0.03  # 3% comisión

            conn_ventas.execute("""
                INSERT INTO FACT_VENTAS (
                    id_venta, id_tiempo, id_producto, id_cliente, id_geografia,
                    id_canal, id_transporte, nro_pedido, cantidad_sacos,
                    cantidad_toneladas, precio_por_saco, total_venta,
                    costo_producto, margen_bruto, descuento_aplicado,
                    moneda, tipo_cambio, estado_venta, fecha_entrega,
                    dias_credito, comision_venta
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                index + 1, id_tiempo, row['id_producto'], row['id_cliente'],
                id_geografia, id_canal, row['id_medio_transporte'], row['nro_pedido'],
                row['cantidad_sacos'], row['cantidad_toneladas'], row['precio_por_saco'],
                row['total_venta'], costo_producto, margen_bruto, descuento_aplicado,
                row['moneda'], row['tipo_cambio'], row['estado_venta'],
                fecha_venta + timedelta(days=random.randint(1, 7)),  # Entrega en 1-7 días
                random.choice([0, 30, 60, 90]),  # Días de crédito
                comision_venta
            ))

        logger.info(f"✅ Tabla de hechos VENTAS poblada con {len(ventas_df)} registros")

    def create_datamart_inventarios(self):
        """Crear datamart de inventarios completo"""
        logger.info("📦 Creando Datamart de INVENTARIOS...")

        conn_inventarios = self.datamart_connections['inventarios']

        # Dimensión Almacén
        conn_inventarios.execute("DROP TABLE IF EXISTS DIM_ALMACEN")
        conn_inventarios.execute("""
        CREATE TABLE DIM_ALMACEN (
            id_almacen INTEGER PRIMARY KEY,
            nombre_almacen TEXT NOT NULL,
            tipo_almacen TEXT,
            ubicacion TEXT,
            capacidad_maxima_ton REAL,
            temperatura_promedio REAL,
            tiene_refrigeracion BOOLEAN,
            responsable TEXT,
            fecha_construccion DATE,
            estado_operativo TEXT
        )
        """)

        # Dimensión Producto (específica para inventarios)
        conn_inventarios.execute("DROP TABLE IF EXISTS DIM_PRODUCTO")
        conn_inventarios.execute("""
        CREATE TABLE DIM_PRODUCTO (
            id_producto INTEGER PRIMARY KEY,
            nombre_producto TEXT NOT NULL,
            tipo_harina TEXT,
            peso_kg REAL,
            categoria TEXT,
            dias_vencimiento INTEGER,
            requiere_refrigeracion BOOLEAN
        )
        """)

        # Tabla de hechos de inventario
        conn_inventarios.execute("DROP TABLE IF EXISTS FACT_INVENTARIO")
        conn_inventarios.execute("""
        CREATE TABLE FACT_INVENTARIO (
            id_inventario INTEGER PRIMARY KEY,
            id_tiempo INTEGER,
            id_producto INTEGER,
            id_almacen INTEGER,
            stock_inicial_ton REAL,
            entradas_ton REAL,
            salidas_ton REAL,
            stock_final_ton REAL,
            stock_minimo_ton REAL,
            stock_maximo_ton REAL,
            valor_unitario REAL,
            valor_total REAL,
            estado_stock TEXT,
            FOREIGN KEY (id_tiempo) REFERENCES DIM_TIEMPO(id_tiempo),
            FOREIGN KEY (id_producto) REFERENCES DIM_PRODUCTO(id_producto),
            FOREIGN KEY (id_almacen) REFERENCES DIM_ALMACEN(id_almacen)
        )
        """)

        # Popular desde BD origen
        almacenes_df = pd.read_sql_query("SELECT * FROM ALMACENES", self.source_conn)
        for _, row in almacenes_df.iterrows():
            conn_inventarios.execute("""
                INSERT INTO DIM_ALMACEN VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row['id_almacen'], row['nombre_almacen'], "Principal",
                row['direccion'], 1000.0, 18.5, True, "Supervisor",
                date(2020, 1, 1), "Operativo"
            ))

        # Popular productos
        productos_df = pd.read_sql_query("SELECT * FROM PRODUCTOS", self.source_conn)
        for _, row in productos_df.iterrows():
            conn_inventarios.execute("""
                INSERT INTO DIM_PRODUCTO VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                row['id_producto'], row['nombre_producto'], "Harina",
                row['peso_kg'], "Molinería", 180, False
            ))

        # Poblar tabla de hechos INVENTARIO desde datos origen
        inventarios_df = pd.read_sql_query("SELECT * FROM INVENTARIOS", self.source_conn)

        if len(inventarios_df) == 0:
            logger.info("📊 No hay datos en INVENTARIOS, creando registros de ejemplo...")
            # Generar algunos registros de ejemplo
            productos_df = pd.read_sql_query("SELECT id_producto FROM PRODUCTOS LIMIT 5", self.source_conn)
            almacenes_df = pd.read_sql_query("SELECT id_almacen FROM ALMACENES LIMIT 3", self.source_conn)

            for i in range(20):  # 20 registros de ejemplo
                fecha_ejemplo = date(2024, 1, 1) + timedelta(days=i*15)
                cursor = conn_inventarios.execute("SELECT id_tiempo FROM DIM_TIEMPO WHERE fecha_completa = ?", (fecha_ejemplo,))
                tiempo_result = cursor.fetchone()
                id_tiempo = tiempo_result[0] if tiempo_result else 1

                id_producto = productos_df.iloc[i % len(productos_df)]['id_producto']
                id_almacen = almacenes_df.iloc[i % len(almacenes_df)]['id_almacen']

                conn_inventarios.execute("""
                    INSERT INTO FACT_INVENTARIO (
                        id_inventario, id_tiempo, id_producto, id_almacen,
                        stock_inicial_ton, entradas_ton, salidas_ton, stock_final_ton,
                        stock_minimo_ton, stock_maximo_ton, valor_unitario, valor_total, estado_stock
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    i + 1, id_tiempo, id_producto, id_almacen,
                    random.uniform(100, 500), random.uniform(20, 100), random.uniform(10, 80),
                    random.uniform(200, 600), 50.0, 800.0, 1500.0, random.uniform(300000, 900000), "Óptimo"
                ))
            logger.info(f"✅ Tabla de hechos INVENTARIO poblada con 20 registros de ejemplo")
        else:
            for index, row in inventarios_df.iterrows():
                # Obtener fecha del inventario
                try:
                    fecha_inventario = pd.to_datetime(row['fecha_registro'], errors='coerce').date()
                    if fecha_inventario is None:
                        fecha_inventario = date(2024, 1, 1)  # Fecha por defecto
                except:
                    fecha_inventario = date(2024, 1, 1)  # Fecha por defecto en caso de error

                # Buscar ID de tiempo correspondiente
                cursor = conn_inventarios.execute("""
                    SELECT id_tiempo FROM DIM_TIEMPO WHERE fecha_completa = ?
                """, (fecha_inventario,))
                tiempo_result = cursor.fetchone()
                id_tiempo = tiempo_result[0] if tiempo_result else 1

                # Usar datos reales de la BD
                stock_inicial = row['stock_inicial_ton']
                entradas = row['entradas_ton']
                salidas = row['salidas_ton']
                stock_final = row['stock_final_ton']
                stock_minimo = row['stock_minimo_ton']
                stock_maximo = row['stock_maximo_ton']
                valor_unitario = row['costo_unitario']
                valor_total = row['valor_total']
                # Usar estado del stock de la BD origen
                estado_stock = row['estado_stock']

                conn_inventarios.execute("""
                    INSERT INTO FACT_INVENTARIO (
                        id_inventario, id_tiempo, id_producto, id_almacen,
                        stock_inicial_ton, entradas_ton, salidas_ton, stock_final_ton,
                        stock_minimo_ton, stock_maximo_ton, valor_unitario, valor_total, estado_stock
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    index + 1, id_tiempo, row['id_producto'], row['id_almacen'],
                    stock_inicial, entradas, salidas, stock_final,
                    stock_minimo, stock_maximo, valor_unitario, valor_total, estado_stock
                ))

            logger.info(f"✅ Tabla de hechos INVENTARIO poblada con {len(inventarios_df)} registros")

        conn_inventarios.commit()
        logger.info("✅ Datamart de INVENTARIOS creado exitosamente")

    def create_datamart_distribucion(self):
        """Crear datamart de distribución completo"""
        logger.info("🚚 Creando Datamart de DISTRIBUCIÓN...")

        conn_distribucion = self.datamart_connections['distribucion']

        # Dimensión Ruta
        conn_distribucion.execute("DROP TABLE IF EXISTS DIM_RUTA")
        conn_distribucion.execute("""
        CREATE TABLE DIM_RUTA (
            id_ruta INTEGER PRIMARY KEY,
            codigo_ruta TEXT NOT NULL,
            descripcion_ruta TEXT,
            distancia_total_km REAL,
            paradas_intermedias INTEGER,
            tiempo_estimado_horas REAL,
            tipo_ruta TEXT,
            requiere_permisos_especiales BOOLEAN,
            costo_base_ruta REAL
        )
        """)

        # Dimensión Estado de Envío
        conn_distribucion.execute("DROP TABLE IF EXISTS DIM_ESTADO_ENVIO")
        conn_distribucion.execute("""
        CREATE TABLE DIM_ESTADO_ENVIO (
            id_estado INTEGER PRIMARY KEY,
            estado_envio TEXT NOT NULL,
            descripcion_estado TEXT,
            es_estado_final BOOLEAN,
            requiere_accion BOOLEAN
        )
        """)

        # Tabla de hechos de distribución
        conn_distribucion.execute("DROP TABLE IF EXISTS FACT_DISTRIBUCION")
        conn_distribucion.execute("""
        CREATE TABLE FACT_DISTRIBUCION (
            id_distribucion INTEGER PRIMARY KEY,
            id_tiempo_salida INTEGER,
            id_tiempo_llegada INTEGER,
            id_ruta INTEGER,
            id_estado_envio INTEGER,
            nro_guia_remision TEXT,
            cantidad_sacos INTEGER,
            cantidad_toneladas REAL,
            costo_transporte REAL,
            costo_total_distribucion REAL,
            dias_transito INTEGER,
            retraso_dias INTEGER,
            entrega_completa BOOLEAN,
            FOREIGN KEY (id_tiempo_salida) REFERENCES DIM_TIEMPO(id_tiempo),
            FOREIGN KEY (id_tiempo_llegada) REFERENCES DIM_TIEMPO(id_tiempo),
            FOREIGN KEY (id_ruta) REFERENCES DIM_RUTA(id_ruta),
            FOREIGN KEY (id_estado_envio) REFERENCES DIM_ESTADO_ENVIO(id_estado)
        )
        """)

        # Agregar rutas de ejemplo
        rutas_ejemplo = [
            (1, "R001", "Lima-Callao", 25.5, 2, 1.5, "Local", False, 150.0),
            (2, "R002", "Lima-Arequipa", 1010.0, 5, 14.0, "Nacional", False, 2500.0),
            (3, "R003", "Lima-Miami", 5500.0, 1, 8.0, "Internacional", True, 8500.0)
        ]

        conn_distribucion.executemany("""
            INSERT INTO DIM_RUTA VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rutas_ejemplo)

        # Estados de envío
        estados_ejemplo = [
            (1, "En Preparación", "Pedido en proceso de preparación", False, True),
            (2, "En Tránsito", "Mercancía en camino", False, False),
            (3, "Entregado", "Mercancía entregada exitosamente", True, False),
            (4, "Devuelto", "Mercancía devuelta al origen", True, True)
        ]

        conn_distribucion.executemany("""
            INSERT INTO DIM_ESTADO_ENVIO VALUES (?, ?, ?, ?, ?)
        """, estados_ejemplo)

        # Poblar tabla de hechos DISTRIBUCION desde datos origen
        distribucion_df = pd.read_sql_query("SELECT * FROM DISTRIBUCION", self.source_conn)

        if len(distribucion_df) == 0:
            logger.info("📊 No hay datos en DISTRIBUCION, creando registros de ejemplo...")
            # Generar algunos registros de ejemplo
            for i in range(15):  # 15 registros de ejemplo
                fecha_ejemplo = date(2024, 1, 1) + timedelta(days=i*20)
                cursor = conn_distribucion.execute("SELECT id_tiempo FROM DIM_TIEMPO WHERE fecha_completa = ?", (fecha_ejemplo,))
                tiempo_result = cursor.fetchone()
                id_tiempo_salida = tiempo_result[0] if tiempo_result else 1
                id_tiempo_llegada = id_tiempo_salida + random.randint(1, 10)

                conn_distribucion.execute("""
                    INSERT INTO FACT_DISTRIBUCION (
                        id_distribucion, id_tiempo_salida, id_tiempo_llegada, id_ruta, id_estado_envio,
                        nro_guia_remision, cantidad_sacos, cantidad_toneladas, costo_transporte,
                        costo_total_distribucion, dias_transito, retraso_dias, entrega_completa
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    i + 1, id_tiempo_salida, id_tiempo_llegada, random.choice([1, 2, 3]), random.choice([1, 2, 3, 4]),
                    f"GR{i+1:06d}", random.randint(50, 500), random.uniform(2.5, 25.0), random.uniform(500, 5000),
                    random.uniform(600, 6000), random.randint(1, 15), random.randint(0, 3), random.choice([True, False])
                ))
            logger.info(f"✅ Tabla de hechos DISTRIBUCIÓN poblada con 15 registros de ejemplo")
        else:
            for index, row in distribucion_df.iterrows():
                # Obtener fechas
                try:
                    fecha_salida = pd.to_datetime(row['fecha_distribucion'], errors='coerce').date()
                    if fecha_salida is None:
                        fecha_salida = date(2024, 1, 1)
                except:
                    fecha_salida = date(2024, 1, 1)

                # Calcular fecha de llegada (agregar días de tránsito)
                dias_transito = random.randint(1, 15)
                fecha_llegada = fecha_salida + timedelta(days=dias_transito)

                # Buscar IDs de tiempo
                cursor = conn_distribucion.execute("""
                    SELECT id_tiempo FROM DIM_TIEMPO WHERE fecha_completa = ?
                """, (fecha_salida,))
                tiempo_salida_result = cursor.fetchone()
                id_tiempo_salida = tiempo_salida_result[0] if tiempo_salida_result else 1

                cursor = conn_distribucion.execute("""
                    SELECT id_tiempo FROM DIM_TIEMPO WHERE fecha_completa = ?
                """, (fecha_llegada,))
                tiempo_llegada_result = cursor.fetchone()
                id_tiempo_llegada = tiempo_llegada_result[0] if tiempo_llegada_result else id_tiempo_salida + 1

                # Asignar ruta según destino
                if "internacional" in str(row.get('destino', '')).lower():
                    id_ruta = 3  # Internacional
                elif "arequipa" in str(row.get('destino', '')).lower():
                    id_ruta = 2  # Nacional
                else:
                    id_ruta = 1  # Local

                # Estado de envío aleatorio
                id_estado_envio = random.choice([1, 2, 3, 4])

                # Calcular métricas
                cantidad_sacos = row.get('cantidad_sacos', random.randint(50, 500))
                cantidad_toneladas = cantidad_sacos * 0.05  # 50kg por saco
                costo_transporte = cantidad_toneladas * random.uniform(80, 200)
                costo_total = costo_transporte * 1.15  # +15% costos adicionales
                retraso_dias = random.randint(0, 3) if id_estado_envio != 3 else 0
                entrega_completa = id_estado_envio == 3

                conn_distribucion.execute("""
                    INSERT INTO FACT_DISTRIBUCION (
                        id_distribucion, id_tiempo_salida, id_tiempo_llegada, id_ruta, id_estado_envio,
                        nro_guia_remision, cantidad_sacos, cantidad_toneladas, costo_transporte,
                        costo_total_distribucion, dias_transito, retraso_dias, entrega_completa
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    index + 1, id_tiempo_salida, id_tiempo_llegada, id_ruta, id_estado_envio,
                    f"GR{index+1:06d}", cantidad_sacos, cantidad_toneladas, costo_transporte,
                    costo_total, dias_transito, retraso_dias, entrega_completa
                ))

            logger.info(f"✅ Tabla de hechos DISTRIBUCIÓN poblada con {len(distribucion_df)} registros")

        conn_distribucion.commit()
        logger.info("✅ Datamart de DISTRIBUCIÓN creado exitosamente")

    def create_datamart_produccion(self):
        """Crear datamart de producción completo"""
        logger.info("🏭 Creando Datamart de PRODUCCIÓN...")

        conn_produccion = self.datamart_connections['produccion']

        # Dimensión Línea de Producción
        conn_produccion.execute("DROP TABLE IF EXISTS DIM_LINEA_PRODUCCION")
        conn_produccion.execute("""
        CREATE TABLE DIM_LINEA_PRODUCCION (
            id_linea INTEGER PRIMARY KEY,
            nombre_linea TEXT NOT NULL,
            tipo_linea TEXT,
            capacidad_ton_dia REAL,
            fecha_instalacion DATE,
            marca_maquinaria TEXT,
            automatizada BOOLEAN,
            numero_operarios_requeridos INTEGER,
            costo_operacion_hora REAL,
            estado_operativo TEXT
        )
        """)

        # Dimensión Turno
        conn_produccion.execute("DROP TABLE IF EXISTS DIM_TURNO")
        conn_produccion.execute("""
        CREATE TABLE DIM_TURNO (
            id_turno INTEGER PRIMARY KEY,
            nombre_turno TEXT NOT NULL,
            hora_inicio TEXT,
            hora_fin TEXT,
            duracion_horas INTEGER,
            supervisor_responsable TEXT,
            es_turno_nocturno BOOLEAN
        )
        """)

        # Tabla de hechos de producción
        conn_produccion.execute("DROP TABLE IF EXISTS FACT_PRODUCCION")
        conn_produccion.execute("""
        CREATE TABLE FACT_PRODUCCION (
            id_produccion INTEGER PRIMARY KEY,
            id_tiempo INTEGER,
            id_linea_produccion INTEGER,
            id_turno INTEGER,
            lote_produccion TEXT,
            cantidad_materia_prima_ton REAL,
            cantidad_producto_terminado_ton REAL,
            cantidad_sacos_producidos INTEGER,
            rendimiento_porcentaje REAL,
            tiempo_produccion_horas REAL,
            costo_total_produccion REAL,
            cumple_estandares_calidad BOOLEAN,
            porcentaje_merma REAL,
            FOREIGN KEY (id_tiempo) REFERENCES DIM_TIEMPO(id_tiempo),
            FOREIGN KEY (id_linea_produccion) REFERENCES DIM_LINEA_PRODUCCION(id_linea),
            FOREIGN KEY (id_turno) REFERENCES DIM_TURNO(id_turno)
        )
        """)

        # Agregar líneas de producción de ejemplo
        lineas_ejemplo = [
            (1, "Línea Harina Panadera", "Panificación", 50.0, date(2020, 1, 15), "Bühler", True, 8, 125.0, "Operativa"),
            (2, "Línea Harina Pastelera", "Pastelería", 30.0, date(2021, 6, 10), "Ocrim", True, 6, 98.0, "Operativa"),
            (3, "Línea Harina Galletera", "Galletería", 40.0, date(2019, 8, 5), "Alapala", False, 10, 110.0, "Operativa")
        ]

        conn_produccion.executemany("""
            INSERT INTO DIM_LINEA_PRODUCCION VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, lineas_ejemplo)

        # Turnos de trabajo
        turnos_ejemplo = [
            (1, "Turno Mañana", "06:00", "14:00", 8, "Juan Pérez", False),
            (2, "Turno Tarde", "14:00", "22:00", 8, "María García", False),
            (3, "Turno Noche", "22:00", "06:00", 8, "Carlos Ruiz", True)
        ]

        conn_produccion.executemany("""
            INSERT INTO DIM_TURNO VALUES (?, ?, ?, ?, ?, ?, ?)
        """, turnos_ejemplo)

        # Poblar tabla de hechos PRODUCCION desde datos origen
        produccion_df = pd.read_sql_query("SELECT * FROM PRODUCCION", self.source_conn)

        if len(produccion_df) == 0:
            logger.info("📊 No hay datos en PRODUCCION, creando registros de ejemplo...")
            # Generar algunos registros de ejemplo
            for i in range(25):  # 25 registros de ejemplo
                fecha_ejemplo = date(2024, 1, 1) + timedelta(days=i*10)
                cursor = conn_produccion.execute("SELECT id_tiempo FROM DIM_TIEMPO WHERE fecha_completa = ?", (fecha_ejemplo,))
                tiempo_result = cursor.fetchone()
                id_tiempo = tiempo_result[0] if tiempo_result else 1

                id_linea = random.choice([1, 2, 3])
                id_turno = random.choice([1, 2, 3])
                cantidad_producida = random.uniform(15, 45)
                cantidad_materia_prima = cantidad_producida * 1.08
                sacos_producidos = int(cantidad_producida * 20)
                rendimiento = (cantidad_producida / cantidad_materia_prima) * 100
                tiempo_produccion = random.uniform(6, 10)
                costo_produccion = cantidad_producida * random.uniform(800, 1200)
                cumple_calidad = random.choice([True, True, True, False])
                porcentaje_merma = ((cantidad_materia_prima - cantidad_producida) / cantidad_materia_prima) * 100

                conn_produccion.execute("""
                    INSERT INTO FACT_PRODUCCION (
                        id_produccion, id_tiempo, id_linea_produccion, id_turno, lote_produccion,
                        cantidad_materia_prima_ton, cantidad_producto_terminado_ton, cantidad_sacos_producidos,
                        rendimiento_porcentaje, tiempo_produccion_horas, costo_total_produccion,
                        cumple_estandares_calidad, porcentaje_merma
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    i + 1, id_tiempo, id_linea, id_turno, f"LOTE{i+1:05d}",
                    cantidad_materia_prima, cantidad_producida, sacos_producidos,
                    rendimiento, tiempo_produccion, costo_produccion,
                    cumple_calidad, porcentaje_merma
                ))
            logger.info(f"✅ Tabla de hechos PRODUCCIÓN poblada con 25 registros de ejemplo")
        else:
            for index, row in produccion_df.iterrows():
                # Obtener fecha de producción
                try:
                    fecha_produccion = pd.to_datetime(row['fecha'], errors='coerce').date()
                    if fecha_produccion is None:
                        fecha_produccion = date(2024, 1, 1)
                except:
                    fecha_produccion = date(2024, 1, 1)

                # Buscar ID de tiempo correspondiente
                cursor = conn_produccion.execute("""
                    SELECT id_tiempo FROM DIM_TIEMPO WHERE fecha_completa = ?
                """, (fecha_produccion,))
                tiempo_result = cursor.fetchone()
                id_tiempo = tiempo_result[0] if tiempo_result else 1

                # Asignar línea de producción según tipo de producto
                id_linea = random.choice([1, 2, 3])  # Rotación entre líneas
                id_turno = random.choice([1, 2, 3])  # Turnos aleatorios

                # Calcular métricas de producción
                cantidad_producida = row.get('cantidad_producida_ton', random.uniform(15, 45))
                cantidad_materia_prima = cantidad_producida * 1.08  # 8% de merma
                sacos_producidos = int(cantidad_producida * 20)  # 20 sacos por tonelada
                rendimiento = (cantidad_producida / cantidad_materia_prima) * 100
                tiempo_produccion = random.uniform(6, 10)  # horas de producción
                costo_produccion = cantidad_producida * random.uniform(800, 1200)
                cumple_calidad = random.choice([True, True, True, False])  # 75% cumple
                porcentaje_merma = ((cantidad_materia_prima - cantidad_producida) / cantidad_materia_prima) * 100

                conn_produccion.execute("""
                    INSERT INTO FACT_PRODUCCION (
                        id_produccion, id_tiempo, id_linea_produccion, id_turno, lote_produccion,
                        cantidad_materia_prima_ton, cantidad_producto_terminado_ton, cantidad_sacos_producidos,
                        rendimiento_porcentaje, tiempo_produccion_horas, costo_total_produccion,
                        cumple_estandares_calidad, porcentaje_merma
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    index + 1, id_tiempo, id_linea, id_turno, f"LOTE{index+1:05d}",
                    cantidad_materia_prima, cantidad_producida, sacos_producidos,
                    rendimiento, tiempo_produccion, costo_produccion,
                    cumple_calidad, porcentaje_merma
                ))

            logger.info(f"✅ Tabla de hechos PRODUCCIÓN poblada con {len(produccion_df)} registros")

        conn_produccion.commit()
        logger.info("✅ Datamart de PRODUCCIÓN creado exitosamente")

    def create_all_datamarts(self):
        """Crear todos los datamarts"""
        try:
            logger.info("🚀 Iniciando creación de datamarts separados...")

            if not self.connect_databases():
                return False

            # Crear dimensión tiempo común
            self.create_dimension_tiempo()

            # Crear cada datamart
            self.create_datamart_ventas()
            self.create_datamart_inventarios()
            self.create_datamart_distribucion()
            self.create_datamart_produccion()

            logger.info("🎉 ¡Todos los datamarts creados exitosamente!")
            return True

        except Exception as e:
            logger.error(f"❌ Error creando datamarts: {e}")
            return False
        finally:
            self.close_connections()

def main():
    """Función principal"""
    print("🏭 CREADOR DE DATAMARTS - EMPRESA MOLINERA")
    print("=" * 60)

    creator = DatamartCreator()

    if creator.create_all_datamarts():
        print("\n✅ PROCESO COMPLETADO EXITOSAMENTE")
        print("\n📁 Archivos de datamarts creados:")
        for name, path in creator.datamart_paths.items():
            if os.path.exists(path):
                size = os.path.getsize(path) / 1024  # KB
                print(f"   • {path} ({size:.1f} KB)")
    else:
        print("\n❌ PROCESO FALLIDO - Ver logs para detalles")

if __name__ == "__main__":
    main()
