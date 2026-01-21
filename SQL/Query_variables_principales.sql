
/*falta crear tabla de SHOCKS: GUERRA EN UCRANIA,SUBA TASAS EN JAPON,INICIO GUERRAS
COMERCIALES USA-CHINA,¿NUEVO INDEC?,CAMBIOS EN LA MEDICIÓN DE IPC,*/
SELECT 
vp.fecha,
DATEDIFF(year,p.inicio,vp.fecha) AS año_mandato,
CASE
	WHEN vp.fecha=p.inicio THEN 1
	ELSE 0
END AS inicio_mandato, 
p.Presidente,
vp.[Reservas internacionales],
vp.[Tipo de cambio minorista (promedio vendedor)],
vp.[Tipo de cambio mayorista de referencia],
vp.[Variación mensual del índice de precios al consumidor],
vp.[Variación interanual del índice de precios al consumidor],
vp.[Préstamos de las entidades financieras al sector privado]
FROM variables_principales AS vp
LEFT JOIN presidencias AS p
ON vp.fecha BETWEEN p.inicio AND p.fin
ORDER BY vp.fecha DESC