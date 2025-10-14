# MATRIZ BUS - DATAMARTS EMPRESA MOLINERA

## 1. DATAMART DE VENTAS

### Dimensiones Conformadas
| Dimensión | Tabla | Descripción |
|-----------|--------|-------------|
| **DIM_TIEMPO** | DIM_TIEMPO | Dimensión temporal con jerarquías año/mes/día |
| **DIM_PRODUCTO** | DIM_PRODUCTO | Catálogo de productos de harina |
| **DIM_CLIENTE** | DIM_CLIENTE | Información de clientes |
| **DIM_GEOGRAFIA** | DIM_GEOGRAFIA | Ubicaciones geográficas (regiones, ciudades) |
| **DIM_CANAL** | DIM_CANAL | Canales de distribución |
| **DIM_TRANSPORTE** | DIM_TRANSPORTE | Medios de transporte |

### Tabla de Hechos: FACT_VENTAS
| Métrica | Descripción | Tipo |
|---------|-------------|------|
| cantidad_sacos | Cantidad de sacos vendidos | Medida Aditiva |
| cantidad_toneladas | Cantidad en toneladas vendidas | Medida Aditiva |
| precio_por_saco | Precio unitario por saco | Medida Semi-aditiva |
| total_venta | Valor total de la venta | Medida Aditiva |
| costo_producto | Costo del producto vendido | Medida Aditiva |
| margen_bruto | Margen de ganancia bruto | Medida Aditiva |
| descuento_aplicado | Descuentos otorgados | Medida Aditiva |
| tipo_cambio | Tipo de cambio aplicado | Medida Semi-aditiva |
| dias_credito | Días de crédito otorgados | Medida Semi-aditiva |
| comision_venta | Comisión por venta | Medida Aditiva |

---

## 2. DATAMART DE INVENTARIOS

### Dimensiones Conformadas
| Dimensión | Tabla | Descripción |
|-----------|--------|-------------|
| **DIM_TIEMPO** | DIM_TIEMPO | Dimensión temporal compartida |
| **DIM_PRODUCTO** | DIM_PRODUCTO | Catálogo de productos compartido |
| **DIM_ALMACEN** | DIM_ALMACEN | Información de almacenes |

### Tabla de Hechos: FACT_INVENTARIO
| Métrica | Descripción | Tipo |
|---------|-------------|------|
| stock_inicial_ton | Stock inicial en toneladas | Medida Semi-aditiva |
| entradas_ton | Entradas al inventario | Medida Aditiva |
| salidas_ton | Salidas del inventario | Medida Aditiva |
| stock_final_ton | Stock final en toneladas | Medida Semi-aditiva |
| stock_minimo_ton | Stock mínimo requerido | Medida Semi-aditiva |
| stock_maximo_ton | Stock máximo permitido | Medida Semi-aditiva |
| valor_unitario | Valor unitario del producto | Medida Semi-aditiva |
| valor_total | Valor total del inventario | Medida Semi-aditiva |

---

## 3. DATAMART DE DISTRIBUCIÓN

### Dimensiones Conformadas
| Dimensión | Tabla | Descripción |
|-----------|--------|-------------|
| **DIM_TIEMPO** | DIM_TIEMPO | Dimensión temporal compartida (salida y llegada) |
| **DIM_RUTA** | DIM_RUTA | Rutas de distribución |
| **DIM_ESTADO_ENVIO** | DIM_ESTADO_ENVIO | Estados del proceso de envío |

### Tabla de Hechos: FACT_DISTRIBUCION
| Métrica | Descripción | Tipo |
|---------|-------------|------|
| cantidad_sacos | Sacos en distribución | Medida Aditiva |
| cantidad_toneladas | Toneladas en distribución | Medida Aditiva |
| costo_transporte | Costo del transporte | Medida Aditiva |
| costo_total_distribucion | Costo total de distribución | Medida Aditiva |
| dias_transito | Días de tránsito | Medida Semi-aditiva |
| retraso_dias | Días de retraso | Medida Semi-aditiva |
| entrega_completa | Indicador de entrega completa | Medida Contable |

---

## 4. DATAMART DE PRODUCCIÓN

### Dimensiones Conformadas
| Dimensión | Tabla | Descripción |
|-----------|--------|-------------|
| **DIM_TIEMPO** | DIM_TIEMPO | Dimensión temporal compartida |
| **DIM_LINEA_PRODUCCION** | DIM_LINEA_PRODUCCION | Líneas de producción |
| **DIM_TURNO** | DIM_TURNO | Turnos de trabajo |

### Tabla de Hechos: FACT_PRODUCCION
| Métrica | Descripción | Tipo |
|---------|-------------|------|
| cantidad_materia_prima_ton | Materia prima utilizada | Medida Aditiva |
| cantidad_producto_terminado_ton | Producto terminado | Medida Aditiva |
| cantidad_sacos_producidos | Sacos producidos | Medida Aditiva |
| rendimiento_porcentaje | Rendimiento del proceso | Medida Semi-aditiva |
| tiempo_produccion_horas | Horas de producción | Medida Aditiva |
| costo_total_produccion | Costo total de producción | Medida Aditiva |
| cumple_estandares_calidad | Cumplimiento de calidad | Medida Contable |
| porcentaje_merma | Porcentaje de merma | Medida Semi-aditiva |

---

## MATRIZ BUS CONSOLIDADA

### Dimensiones Conformadas Compartidas

| Dimensión | Ventas | Inventarios | Distribución | Producción |
|-----------|--------|-------------|--------------|------------|
| **DIM_TIEMPO** | ✅ | ✅ | ✅ | ✅ |
| **DIM_PRODUCTO** | ✅ | ✅ | ❌ | ❌ |
| **DIM_CLIENTE** | ✅ | ❌ | ❌ | ❌ |
| **DIM_GEOGRAFIA** | ✅ | ❌ | ❌ | ❌ |
| **DIM_CANAL** | ✅ | ❌ | ❌ | ❌ |
| **DIM_TRANSPORTE** | ✅ | ❌ | ❌ | ❌ |
| **DIM_ALMACEN** | ❌ | ✅ | ❌ | ❌ |
| **DIM_RUTA** | ❌ | ❌ | ✅ | ❌ |
| **DIM_ESTADO_ENVIO** | ❌ | ❌ | ✅ | ❌ |
| **DIM_LINEA_PRODUCCION** | ❌ | ❌ | ❌ | ✅ |
| **DIM_TURNO** | ❌ | ❌ | ❌ | ✅ |

### Procesos de Negocio vs Dimensiones

| Proceso | Tabla de Hechos | Granularidad | Dimensiones Principales |
|---------|-----------------|--------------|------------------------|
| **Ventas** | FACT_VENTAS | Transacción por pedido | Tiempo, Producto, Cliente, Geografía |
| **Inventarios** | FACT_INVENTARIO | Snapshot diario por producto-almacén | Tiempo, Producto, Almacén |
| **Distribución** | FACT_DISTRIBUCION | Envío por guía de remisión | Tiempo, Ruta, Estado Envío |
| **Producción** | FACT_PRODUCCION | Lote de producción | Tiempo, Línea Producción, Turno |

---

## MATRIZ BUS DETALLADA POR DATAMART

### 1. DATAMART DE VENTAS - MATRIZ MÉTRICA-DIMENSIÓN

| **MÉTRICAS** | **DIM_TIEMPO** | | | | | | | | | | | | **DIM_PRODUCTO** | | | | | | | | **DIM_CLIENTE** | | | | | | | | | **DIM_GEOGRAFIA** | | | | | **DIM_CANAL** | | | | | **DIM_TRANSPORTE** | | | | |
|--------------|----------------|---|---|---|---|---|---|---|---|---|---|---|------------------|---|---|---|---|---|---|---|-----------------|---|---|---|---|---|---|---|---|-------------------|---|---|---|---|---------------|---|---|---|---|-------------------|---|---|---|---|
| | **Año** | **Mes** | **Día** | **Trimestre** | **Nombre_Mes** | **Día_Semana** | **Número_Semana** | **Periodo_Fiscal** | **Es_Fin_Semana** | **Es_Feriado** | **Turno_Trabajo** | **Periodo_Estacional** | **Nombre_Producto** | **Tipo_Harina** | **Peso_Kg** | **Categoría** | **Subcategoría** | **Marca** | **Es_Premium** | **Unidad_Medida** | **Nombre_Cliente** | **Tipo_Cliente** | **Segmento** | **Tamaño_Empresa** | **Id_País** | **Nombre_País** | **Región** | **Fecha_Registro** | **Estado_Cliente** | **País** | **Región** | **Continente** | **Zona_Comercial** | **Es_Mercado_Principal** | **Canal_Distribución** | **Tipo_Canal** | **Descripción** | **Comisión_%** | **Es_Activo** | **Medio_Transporte** | **Tipo_Transporte** | **Costo_Promedio_Km** | **Capacidad_Ton** | **Es_Internacional** |
| **cantidad_sacos** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ | ❌ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ |
| **cantidad_toneladas** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ |
| **precio_por_saco** | ❌ | ✅ | ❌ | ✅ | ✅ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **total_venta** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| **costo_producto** | ✅ | ✅ | ❌ | ✅ | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ❌ | ✅ | ❌ | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ |
| **margen_bruto** | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **descuento_aplicado** | ❌ | ✅ | ❌ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ | ❌ | ✅ | ✅ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **comision_venta** | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ❌ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |

---

### 2. DATAMART DE INVENTARIOS - MATRIZ MÉTRICA-DIMENSIÓN

| **MÉTRICAS** | **DIM_TIEMPO** | | | | | | | | | | | | | **DIM_PRODUCTO** | | | | | | | | **DIM_ALMACEN** | | | | | | | | | |
|--------------|----------------|---|---|---|---|---|---|---|---|---|---|---|---|------------------|---|---|---|---|---|---|---|-----------------|---|---|---|---|---|---|---|---|---|
| | **Año** | **Mes** | **Día** | **Trimestre** | **Nombre_Mes** | **Día_Semana** | **Número_Semana** | **Periodo_Fiscal** | **Es_Fin_Semana** | **Es_Feriado** | **Turno_Trabajo** | **Periodo_Estacional** | **Fecha_Completa** | **Nombre_Producto** | **Tipo_Harina** | **Peso_Kg** | **Categoría** | **Subcategoría** | **Marca** | **Es_Premium** | **Unidad_Medida** | **Nombre_Almacén** | **Tipo_Almacén** | **Ubicación** | **Capacidad_Máx_Ton** | **Temp_Promedio** | **Tiene_Refrigeración** | **Responsable** | **Fecha_Construcción** | **Estado_Operativo** |
| **stock_inicial_ton** | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ |
| **entradas_ton** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ | ❌ | ✅ |
| **salidas_ton** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ | ❌ | ✅ |
| **stock_final_ton** | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ |
| **stock_minimo_ton** | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ❌ | ✅ |
| **stock_maximo_ton** | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ❌ | ✅ |
| **valor_unitario** | ❌ | ✅ | ❌ | ✅ | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **valor_total** | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ | ❌ | ✅ |

---

### 3. DATAMART DE DISTRIBUCIÓN - MATRIZ MÉTRICA-DIMENSIÓN

| **MÉTRICAS** | **DIM_TIEMPO_SALIDA** | | | | | | | | | | | | **DIM_TIEMPO_LLEGADA** | | | | | | | | | | | | **DIM_RUTA** | | | | | | | | **DIM_ESTADO_ENVIO** | | | |
|--------------|----------------------|---|---|---|---|---|---|---|---|---|---|---|----------------------|---|---|---|---|---|---|---|---|---|---|---|--------------|---|---|---|---|---|---|---|---------------------|---|---|---|
| | **Año** | **Mes** | **Día** | **Trimestre** | **Nombre_Mes** | **Día_Semana** | **Número_Semana** | **Periodo_Fiscal** | **Es_Fin_Semana** | **Es_Feriado** | **Turno_Trabajo** | **Periodo_Estacional** | **Año** | **Mes** | **Día** | **Trimestre** | **Nombre_Mes** | **Día_Semana** | **Número_Semana** | **Periodo_Fiscal** | **Es_Fin_Semana** | **Es_Feriado** | **Turno_Trabajo** | **Periodo_Estacional** | **Código_Ruta** | **Descripción_Ruta** | **Distancia_Km** | **Paradas_Inter** | **Tiempo_Est_Hrs** | **Tipo_Ruta** | **Requiere_Permisos** | **Costo_Base** | **Estado_Envío** | **Descripción_Estado** | **Es_Estado_Final** | **Requiere_Acción** |
| **cantidad_sacos** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ |
| **cantidad_toneladas** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ |
| **costo_transporte** | ✅ | ✅ | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |
| **costo_total_distribucion** | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **dias_transito** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ❌ | ❌ | ❌ |
| **retraso_dias** | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ |
| **entrega_completa** | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ |

---

### 4. DATAMART DE PRODUCCIÓN - MATRIZ MÉTRICA-DIMENSIÓN

| **MÉTRICAS** | **DIM_TIEMPO** | | | | | | | | | | | | **DIM_LINEA_PRODUCCION** | | | | | | | | | **DIM_TURNO** | | | | | |
|--------------|----------------|---|---|---|---|---|---|---|---|---|---|---|--------------------------|---|---|---|---|---|---|---|---|---------------|---|---|---|---|---|
| | **Año** | **Mes** | **Día** | **Trimestre** | **Nombre_Mes** | **Día_Semana** | **Número_Semana** | **Periodo_Fiscal** | **Es_Fin_Semana** | **Es_Feriado** | **Turno_Trabajo** | **Periodo_Estacional** | **Nombre_Línea** | **Tipo_Línea** | **Capacidad_Ton_Día** | **Fecha_Instalación** | **Marca_Maquinaria** | **Automatizada** | **Núm_Operarios_Req** | **Costo_Operación_Hora** | **Estado_Operativo** | **Nombre_Turno** | **Hora_Inicio** | **Hora_Fin** | **Duración_Hrs** | **Supervisor_Responsable** | **Es_Nocturno** |
| **cantidad_materia_prima_ton** | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ | ❌ | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ | ❌ |
| **cantidad_producto_terminado_ton** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ | ✅ |
| **cantidad_sacos_producidos** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ | ❌ | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ | ✅ |
| **rendimiento_porcentaje** | ❌ | ✅ | ❌ | ✅ | ✅ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ |
| **tiempo_produccion_horas** | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **costo_total_produccion** | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ |
| **cumple_estandares_calidad** | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ |
| **porcentaje_merma** | ❌ | ✅ | ❌ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ | ❌ |

---

## LEYENDA DE LA MATRIZ
- ✅ = **Fuerte Relación**: La métrica se puede analizar significativamente por esta dimensión
- ❌ = **Sin Relación**: La métrica no se beneficia del análisis por esta dimensión

---

## BENEFICIOS DE ESTA ARQUITECTURA

1. **Dimensiones Conformadas**: `DIM_TIEMPO` y `DIM_PRODUCTO` son compartidas entre datamarts
2. **Independencia**: Cada datamart puede evolucionar según necesidades específicas
3. **Performance**: Cada datamart optimizado para su dominio específico
4. **Escalabilidad**: Fácil agregar nuevos datamarts o modificar existentes
5. **Consistencia**: Las dimensiones conformadas garantizan consistencia en reportes cruzados

---

## NOTAS TÉCNICAS

- **Granularidad**: Cada tabla de hechos tiene su propia granularidad apropiada
- **Medidas Aditivas**: Se pueden sumar a través de todas las dimensiones
- **Medidas Semi-aditivas**: Se pueden sumar a través de algunas dimensiones (no tiempo)
- **Medidas Contables**: Conteos y flags booleanos
- **Dimensiones Conformadas**: Garantizan consistencia entre datamarts
