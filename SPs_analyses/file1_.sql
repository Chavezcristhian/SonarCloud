SET ANSI_NULLS, QUOTED_IDENTIFIER ON
GO
CREATE or alter procedure dbo.proc_abmgir_Pago_entrega_remesa
    @I_origen               tinyint         = 0,                    -- 0 = llamado desde pantalla    1 = llamado desde el Automatico 3 = Llamado desde WS
    @I_grabar               int             = 0,
    @I_clave                varchar(30)     = '',                   -- la clave viene con las iniciales de la remesadora, no deben haber claves
    @I_remesadora           int             = 0,
    @I_cliente              int             = 0,                    -- cliente /eventual
    @I_ciudad_exp_doc       varchar(100)    = '',
    @I_fecha_venc_doc       varchar(10)     = '',
    @I_efectivo             decimal(13, 2)  = 0,
    @I_sistema_via          smallint        = 0,
    @I_cuenta_via           decimal(10)     = 0,
    @I_observaciones        varchar(100)    = '',
    @I_destino              varchar(300)    = '',
    @I_motivo               varchar(300)    = '',
    @I_usuario              int             = NULL,
    @O_date_time_UTC        datetime        = NULL          output,
    @O_date_time_local      datetime        = NULL          output,
    @O_tipo_iden            varchar(50)     = ''            output,
    @O_nro_identificacion   varchar(14)     = ''            output,
    @O_fecha_venc_identi    smalldatetime   = '01-01-1900'  output,
    @O_codigo_remesa        varchar(50)     = ''            output,
    @O_nro_registro         int             = 0             output,
    @O_id_trn               varchar(50)     = ''            output,
    @O_tipo_conexion        tinyint         = 0             output,
    @O_string_xml           varchar(max)    = ''            output,
    @O_cuerpo_xml           varchar(max)    = ''            output,
    @O_head_ini_xml         varchar(max)    = ''            output,
    @O_head_fin_xml         varchar(max)    = ''            output,
    @O_metodo_exe           varchar(400)    = ''            output,
    @O_comprobante          decimal(16)     = 0             output,
    @O_error_msg            varchar(300)    = ''            output
WITH ENCRYPTION AS
/************************************************************************************************/
/*  DESCRIPCION: Permite registrar el pago o entrega de la remesa                               */
/*  OBSERVACIONES: RD-XXXX Pase X                                                               */
/*  REV.CALIDAD:                                                                              */
/************************************************************************************************/
BEGIN
    declare
        @F_fecha_proceso smalldatetime,
        @F_contable decimal(8, 6),
        @F_comprobante decimal(16),
        @F_moneda tinyint,
        @F_moneda_pago tinyint,
        @F_estado tinyint,
        @F_desc_moneda varchar(5),
        @F_oficina tinyint,
        @F_agencia int,
        @F_linea varchar(100),
        @F_linea1 varchar(100),
        @F_linea2 varchar(100),
        @F_linea3 varchar(100),
        @F_linea4 varchar(100),
        @F_linea5 varchar(100),
        @F_rowcount smallint,
        @F_sec int,
        @F_sec1 int,
        @F_usuario int,
        @F_fecha_today smalldatetime,
        @F_observacion_error varchar(200),
        @F_mensaje varchar(100),
        @F_error_exec smallint,
        @F_error smallint,
        @F_codigo_remesa varchar(30),
        @F_nombre_ful_remitente varchar(100),
        @F_nombre_ful_Beneficiario varchar(100),
        @F_pais_remitente varchar(100),
        @F_ciudad_beneficiario varchar(200),--int,--varchar(30),
        @F_telefono1 varchar(30),
        @F_telefono2 varchar(30),
        @F_forma_pago smallint,
        @F_cuenta decimal(16),
        @F_Importe decimal(13,2),
        @F_Remesadora int,
        @F_concepto int,
        @F_desc_concepto varchar(100),
        @F_concepto_remesa int,
        @F_nombre_cliente varchar(70),
        @F_nombre_remesadora varchar(100),
        @F_importe_ope decimal(13,2),
        @F_cod_tran smallint ,
        @F_moneda_via tinyint,
        @F_cliente_cah varchar(70),
        @F_monto_pagado decimal(13,2),
        @F_importe_total decimal(13,2),
        @F_sigla_mda varchar(6),
        @F_moneda_entrada tinyint,
        @f_recibe tinyint,
        @F_imp_conv_caja decimal(13,2),
        @F_imp_conv_cah decimal(13,2),
        @F_saldo_deposito decimal(13,2),
        @F_saldo_limite decimal(13,2),
        @F_pagar decimal(13,2),
        @F_aux_saldo decimal(13,2),
        @F_nro_registro decimal(16,0),
        @F_x_pagar decimal(13,2),
        @F_total_pagar decimal(13,2),
        @F_pagado decimal(13,2),
        @f_viaspago varchar(400),
        @F_tipo_comision tinyint,
        @F_comision decimal(13,2),
        @F_comision_pendiente decimal(13,2),
        @F_total_pagar_comision decimal(13,2),
        @F_aplica tinyint,
        @F_apellido_beneficiario varchar(100),
        @F_nombre_beneficiario varchar(100),
        @F_nombre_full_cliente varchar(100),
        @F_nombre1 varchar(100),
        @F_apellido1 varchar(100),
        @F_apellido2 varchar(25),
        @F_apellido3 varchar(25),
        @F_importe_sus decimal(13,2),
        @f_nomb_concide tinyint,
        @f_ap_concide tinyint,
        @F_moneda_dep tinyint,
        @F_moneda_aux tinyint,
        @F_cant_registro decimal(16),
        @F_cant_registro2 decimal(16),
        @F_estado_cabecera tinyint,
        @F_estado_head tinyint,
        @F_monto_total decimal(13,2),
        @F_moneda_limite_cons tinyint ,
        @F_comprobanteF1 decimal(12),
        @F_dpto_usuario char(2),
        @F_autorizacion int,
        @F_fecha_venc smalldatetime,
        @F_cli_eventual tinyint,
        @F_cliente_cah2 int,
        @F_tipo_usuario tinyint,
        @F_moneda_comis tinyint,
        @F_monto_limite decimal(13,2),
        @F_comprobante_auxpcc decimal(16),
        @F_tipo_transaccion tinyint,
        @F_fecha_desde_log datetime,
        @F_subzona decimal(9),
        @F_tesorero int,
        @F_valor_minimo decimal(13, 2)      = 0.0,
        @F_valor_maximo decimal(13, 2)      = 0.0,
        @F_tipo_ident_cli int,
        @F_tipo_ident varchar(45),
        @F_identificacion varchar(14),
        @F_telefono_eventual varchar(20),
        @F_direccion_eventual varchar(30),
        @F_char char(1),
        @F_cadena varchar(70),
        @F_cant int,
        @F_ok tinyint,
        @F_estado_envio2 tinyint,
        @F_estado_envio tinyint,
        @F_id_trn varchar(50),
        @F_Nro_giro varchar(100),
        @F_Id_transaccion decimal(12),
        @F_metodo varchar(400),
        @F_str_pais_emitido_ident varchar(100),
        @F_tipo_identificacion tinyint,
        @F_pais_emitido_ident int,
        @F_sigla_ciudad_emision char(2),
        @F_existe varchar(100),
        @F_str_input_mensaje varchar(max),
        @F_origen_carga tinyint,
        @F_agenca_usr int,
        @F_proc_aut tinyint,
        @F_usr_webservice varchar(100) = NULL,
        @F_pasw_webservice varchar(400) = NULL,
        @F_url_dmz varchar(max) = NULL,
        @F_url_externa varchar(max) = NULL,
        @F_fecha_nac smalldatetime = NULL,
        @F_nombcorto varchar(70),
        @F_ciudad int,
        @F_comprobante_caja decimal(16),
        @F_str_ciudad_bene varchar(100),
        @F_ciudad_remitente varchar(200),
        @F_sigla_pais_benef varchar(10),
        @F_caddeposito varchar(200) = ltrim(rtrim(concat(rtrim(@I_clave), '|', ltrim(str(@I_remesadora, 10))))),
        @F_nivel_ingreso   int=0
    ---------------------------------------------------------------------------------------------------
    --  SENTENCIAS FIJAS
    ---------------------------------------------------------------------------------------------------
    set nocount on
    set XACT_ABORT ON

    If @@NESTLEVEL = 1
    begin
        if @@trancount > 0
            rollback tran
        select nombre_reporte = 'SinReporte'
    end

    select
        @F_fecha_proceso = (select fecha_proceso from pam_fechaproceso where indicador = 'A' and sistema = 400),
        @F_fecha_today = getdate(),
        @F_fecha_desde_log = getdate(),
        @O_date_time_local = getdate(),
        @O_date_time_UTC = GETUTCDATE()

    select
        @F_usuario = cu.cliente,
        @F_agencia = cu.agencia,
        @F_subzona = cu.subzona,
        @F_tesorero= t.cliente,
        @F_oficina = cu.oficina,
        @F_tipo_usuario = cu.tipo_usuario,
        @F_dpto_usuario = pa.dpto,--07
        @F_nombcorto = cu.nombcorto
    from climov_usuario as cu
        join pammst_agencia as pa on pa.oficina = cu.oficina
            AND pa.agencia = cu.agencia
            and pa.indicador = 'A'
            and cu.fecha_proceso between pa.fecha_proceso and pa.fecha_proceso_hasta
            and pa.tipo_agencia in(1, 3)
        join pam_subzona psz on psz.subzona = cu.subzona
            and cu.fecha_proceso between psz.fecha_proceso and psz.fecha_proceso_hasta
            and psz.indicador = 'A'
        left join pamcaj_tesorero_activo t on t.subagencia = psz.subagencia
            and cu.fecha_proceso between t.fecha_proceso and t.fecha_proceso_hasta
            and t.indicador = 'A'
    where cu.nombcorto = CONVERT(varchar, SYSTEM_USER)
        and cu.indicador = 'A'
        and cu.fecha_proceso_hasta = '20500101'

    set @F_tesorero = ISNULL(@F_tesorero, 0)

    select @F_contable = contable from pam_dolar
    where @F_fecha_proceso between fecha_proceso and fecha_proceso_hasta
        and indicador = 'A'

    if @@NESTLEVEL = 1 and @I_grabar = 1
    begin
        begin tran
    end

    set @O_string_xml = ''
    ------------------------------------------------------------------------------
    -- VALIDACIONES DE CAJA
    ------------------------------------------------------------------------------
    If @F_usuario is null
    begin
        select
            @F_usuario = 0,
            @O_error_msg = 'El usuario es Incorrecto, Verifique...'+system_user+ str(ISNULL(@F_usuario, -1))

        goto linea_error
    end

    set @F_proc_aut = iif(@I_origen = 1, 1, 0)

    if @I_origen = 1--- Para los procesos automaticos la agencia de la transaccion debe ser la agencia de la cuenta.
    begin
        if @I_sistema_via = 200
        begin
            select @F_agencia = agencia
            from cahmst_maestro
            where nro_cuenta = @I_cuenta_via
                and indicador = 'A'
        end
    end

    if isnull(@F_agencia, 0) < = 0
        set @F_agencia = @F_agenca_usr

    --Validar parametro
    select
        @F_valor_minimo = MIN(valor),
        @F_valor_maximo = MAX(valor)
    from pam_tablas
    where tabla = 116
        and fecha_proceso_hasta = '01-01-2050'
        and indicador = 'A'
    -------------------------------------------------------------------------
    -- VALIDAR EL NOMBRE
    -------------------------------------------------------------------------
    set @F_importe_total = 0
    -------------------------------------------------------------------------
    -- VALIDAR el codigo de la remesadora
    -------------------------------------------------------------------------
    If LEN(LTRIM(rtrim(@I_clave))) < = 0
    begin
        set @O_error_msg = 'Ingresar la clave de remesa'
        goto linea_error
    end

    If @I_origen = 3 --Si es por WS
    Begin
        select
            @F_estado = estado,
            @F_codigo_remesa = codigo_remesa,
            @F_nro_registro = nro_registro,
            @F_nombre_ful_remitente = nombre_ful_remitente,
            @F_nombre_ful_Beneficiario = nombre_ful_Beneficiario,
            @F_apellido_beneficiario = UPPER(apellido_Beneficiario),
            @F_nombre_beneficiario = UPPER(Nombre_Beneficiario),
            @F_pais_remitente = pais_remitente,
            @F_ciudad_beneficiario = ciudad_beneficiario,
            @F_telefono1 = telefono1,
            @F_telefono2 = telefono2,
            @F_forma_pago = forma_pago,
            @F_cuenta = cuenta,
            @F_Importe = importe,
            @F_moneda_pago = moneda,
            @F_Remesadora = remesadora,
            @F_id_trn = codigo_ID_trans,
            @F_Nro_giro = Nro_giro,
            @F_origen_carga = 2
        from #girmst_detalle as a
        Where rtrim(ltrim(clave_trn)) = rtrim(ltrim(@I_clave))
            and remesadora = @I_remesadora

        select @F_error = @@error, @F_rowcount = @@rowcount
    end
    else
    Begin
        -- Validar si hay una clave Pendiente
        If Exists(select 1 from girmst_detalle as a Where clave_trn = rtrim(ltrim(@I_clave)) and remesadora = @I_remesadora and fecha_proceso_hasta = '01-01-2050' and indicador = 'A')
        begin
            select top 1
                @F_estado = estado,
                @F_codigo_remesa = codigo_remesa,
                @F_nro_registro = nro_registro,
                @F_nombre_ful_remitente = nombre_ful_remitente,
                @F_nombre_ful_Beneficiario = nombre_ful_Beneficiario ,
                @F_apellido_beneficiario = UPPER(apellido_Beneficiario),
                @F_nombre_beneficiario = UPPER(nombre_Beneficiario),
                @F_pais_remitente = pais_remitente,
                @F_ciudad_remitente = ciudad_remitente,
                @F_ciudad_beneficiario = ciudad_beneficiario,
                @F_telefono1 = telefono1,
                @F_telefono2 = telefono2,
                @F_forma_pago = forma_pago,
                @F_cuenta = cuenta,
                @F_Importe = Importe,
                @F_moneda_pago = moneda,
                @F_estado_envio = estado_envio,
                @F_id_trn = id_trn,
                @F_Nro_giro = nro_giro,
                @F_origen_carga = origen_carga
            from girmst_detalle as a
            Where clave_trn = rtrim(ltrim(@I_clave))
                and remesadora = @I_remesadora
                and fecha_proceso_hasta = '01-01-2050'
                and indicador = 'A'
            order by fecha_alta desc
            select @F_error = @@error, @F_rowcount = @@rowcount
        end
        -- Si no existe la clave
        Else
        begin
            select @F_error = 0, @F_rowcount = 0
        end
        --Asignar la clave
        set @F_Remesadora = @I_remesadora
    end
    --Validacion de la remesas
    If @F_error <> 0 Or @F_rowcount > 1
    begin
        set @O_error_msg = concat('Error al leer girmst_detalle .error = ', rtrim(cast(@F_error as char(7))), ',rowcount = ', rtrim(cast(@F_rowcount as char(7))))
        GOTO linea_Error
    end

    If @F_rowcount = 0
    begin
        set @O_error_msg = 'La Clave de la remesa no existe en la remesadora seleccionada, Verifique...'
        goto linea_error
    end

    If @F_estado <> 0
    begin
        set @O_error_msg = 'La remesa se encuentra pagada, Verifique con el Encargado Operativo'
        goto linea_error
    end

    set @F_estado_envio2 = IIF(@F_estado_envio = 1, 1, 2)
    --Validar que ingresen el codigo de la remesadora
    If isnull(@F_remesadora, 0) < = 0
    begin
        set @O_error_msg = 'Ingresar el código de la remesadora'
        goto linea_error
    end
    -- VALIDAR LA REMESADORA
    exec @F_error_exec = proc_gir_validar_remesadora
        @I_origen = 0,--@F_tipo_Origen,
        @I_fecha_proceso = @F_fecha_proceso,
        @I_remesadora = @F_remesadora,
        @O_nombre_remesadora = @F_nombre_remesadora output,
        @O_tipo_comision = @F_tipo_comision output,--1 = DIARIO 2 = MENSUAL
        @O_moneda_dep = @F_moneda_dep output,--1 = $us 2 = BS y $us
        @O_moneda_limite_cons = @F_moneda_limite_cons output,--1 = $us 2 = BS y $us
        @O_tipo_conexion = @O_tipo_conexion output,
        @O_error_msg = @O_error_msg output

    IF @@error <> 0
    begin
        set @O_error_msg = 'Error al ejecutar proc_gir_validar_remesadora'
        goto linea_error
    end

    If @F_error_exec <> 0 goto linea_error
    ----------------------------------------------------------------------
    -- VALIDAR LA CABECERA
    ----------------------------------------------------------------------
    If @I_origen <> 3
    begin
        select
            @F_cant_registro = cant_reg,
            @F_estado_head = estado,
            @F_fecha_venc = fecha_vencimiento
        from girmst_head as a
        Where a.remesadora = @F_remesadora
            and a.codigo_remesa = @F_codigo_remesa
            and a.fecha_proceso_hasta = '01-01-2050'
            and a.indicador = 'A'
        select @F_error = @@error, @F_rowcount = @@rowcount

        If @F_error <> 0 Or @F_rowcount > 1
        begin
            set @O_error_msg = concat('Error al leer girmst_head .error = ', rtrim(cast(@F_error as char(7))), ',rowcount = ', rtrim(cast(@F_rowcount as char(7))))
            GOTO linea_Error
        end

        If @F_rowcount = 0
        begin
            set @O_error_msg = 'El detalle de la remesa no tiene la cabecera del archivo, Verifique...'
            goto linea_error
        end
        -- validar la fecha de vencimiento de la cabecera
        If @F_fecha_proceso > @F_fecha_venc
        begin
            set @O_error_msg = 'La Remesa se encuentra vencida, Verifique...'
            goto linea_error
        end
    end --Fin de la Validacion de Cabecera
    -- VALIDAR EL CLIENTE O CLIENTE EVENTUAL
    If isnull(@I_cliente, -1) < = 0
    begin
        set @O_error_msg = 'Ingresar el código del Cliente/Eventual'
        goto linea_error
    end

    set @F_cli_eventual = 0
    Select
        @F_nombre_full_cliente = a.nombre_ful,
        @F_nombre1 = ltrim(b.nombre1),
        @F_apellido1 = ltrim(rtrim(concat(rtrim(b.apellido1), ' ', rtrim(ltrim(b.apellido2)), ' ', ltrim(b.apellido3)))),
        @F_apellido2 = ltrim(b.apellido2),
        @F_apellido3 = ltrim(b.apellido3),
        @F_tipo_identificacion = a.tipo_ident,
        @F_tipo_ident = ltrim(c.descripcion),
        @F_tipo_ident_cli = c.tipo_id,
        @F_identificacion = ltrim(a.identificacion),
        @F_pais_emitido_ident = pais_emitido_ident,
        @F_fecha_nac = a.fecha_nac
    from climst_cliente as a,
        climst_persona as b,
        pam_identificacion as c
    where a.cliente = @I_cliente
        and a.fecha_proceso_hasta = '01-01-2050'
        and a.indicador = 'A'
        and a.cliente = b.cliente
        and a.fecha_proceso between b.fecha_proceso AND b.fecha_proceso_hasta
        and b.indicador = 'A'
        and a.tipo_ident = c.tipo_id
        and c.indicador = 'A'

    select @F_error = @@error, @F_rowcount = @@rowcount
    If @F_error <> 0 Or @F_rowcount > 1
    begin
        set @O_error_msg = concat('Error al leer climst_cliente .error = ', rtrim(cast(@F_error as char(7))), ',rowcount = ', rtrim(cast(@F_rowcount as char(7))))
        GOTO linea_Error
    end
    -- Si es Cliente
    If @F_rowcount = 1 and @I_origen in(0, 3) and @@NESTLEVEL in(1, 2)
    begin
        If not exists(select 1 from climst_telefonos where cliente = @I_cliente and indicador = 'A')
        begin
            set @O_error_msg = 'Debe Actualizar los datos del cliente (Nombre, Apellido Paterno, Apellido Materno, Identificación y Teléfono)'
            goto linea_error
        end
        If not exists(select 1 from pam_pais where pais = @F_pais_emitido_ident and indicador = 'A')
        begin
            set @O_error_msg = 'Debe Actualizar los datos del cliente (Nombre, Apellido Paterno, Apellido Materno, Identificación y Teléfono, País(Emisión) )'
            goto linea_error
        end
    end

    If @F_rowcount = 0 --Si no es Cliente verificar si es Cliente eventual
    begin
        set @F_cli_eventual = 1
        Select
            @F_nombre_full_cliente = nombre_ful,
            @F_nombre1 = ltrim(nombre1),
            @F_apellido1 = ltrim(rtrim(concat(rtrim(apellido1), ' ', ltrim(apellido2)))),
            @F_apellido2 = ltrim(apellido2),
            @F_apellido3 = ltrim(apellido1),
            @F_tipo_identificacion = a.tipo_ident,
            @F_tipo_ident = ltrim(b.descripcion),
            @F_tipo_ident_cli = b.tipo_id,
            @F_identificacion = ltrim(a.identificacion),
            @F_telefono_eventual = ltrim(telefono),
            @F_direccion_eventual = ltrim(direccion),
            @F_pais_emitido_ident = a.pais_emitido_ident,
            @F_fecha_nac = '01/01/1900'
        from climst_eventual as a,
            pam_identificacion as b--
        where a.cliente = @I_cliente
            and a.indicador = 'A'
            and a.tipo_ident = b.tipo_id
            and b.indicador = 'A'

        select @F_error = @@error, @F_rowcount = @@rowcount
        If @F_error <> 0 Or @F_rowcount >1
        begin
            set @O_error_msg = concat('Error al leer climst_eventual .error = ', rtrim(cast(@F_error as char(7))), ',rowcount = ', rtrim(cast(@F_rowcount as char(7))))
            GOTO linea_Error
        end

        If @F_rowcount = 0
        begin
            set @O_error_msg = 'El código del cliente/eventual no existe'
            goto linea_error
        end
        -- Validar SI HAY NUMERICO EN EL NOMBRE Y APELLIDO DEL CLIENTE EVENTUAL
        set @F_cadena = rtrim(ltrim(@F_nombre1))
        select @F_cant = len(@F_cadena),
            @F_sec1 = 1,
            @F_ok = 1

        while @F_sec1 < = @F_cant and @F_ok = 1-- recorer toda la cadena -- y ademas no tenga numerico
        begin
            set @F_char = SUBSTRING(@F_cadena, @F_sec1, 1)
            if not (@F_char like '[A-Z]' or @F_char like ' ')
                set @F_ok = 0
            set @F_sec1 += 1
        end --while @F_sec < = @F_cant
        If @F_ok = 1
        Begin
            -- Verificar si el apellido paterno tiene numero
            set @F_cadena = rtrim(ltrim(@F_apellido3))
            select @F_cant = len(@F_cadena),
                @F_sec1 = 1,
                @F_ok = 1

            while @F_sec1 < = @F_cant and @F_ok = 1-- y ademas no tenga numerico
            begin
                set @F_char = SUBSTRING(@F_cadena, @F_sec1, 1)
                if not (@F_char like '[A-Z]' or @F_char like ' ')
                    set @F_ok = 0
                set @F_sec1 += 1
            end --while @F_sec < = @F_cant
        end
        If @F_ok = 1
        Begin
            -- Verificar si el apellido materno tiene numero
            set @F_cadena = rtrim(ltrim(@F_apellido2))
            select @F_cant = len(@F_cadena),
                @F_sec1 = 1,
                @F_ok = 1

            while @F_sec1 < = @F_cant and @F_ok = 1-- y ademas no tenga numerico
            begin
                set @F_char = SUBSTRING(@F_cadena, @F_sec1, 1)
                if not (@F_char like '[A-Z]' or @F_char like ' ')
                    set @F_ok = 0
                set @F_sec1 += 1
            end --while @F_sec1 < = @F_cant
        end
        -- VALIDAR QUE SE TENGA LOS DATOS DE CLIENTE EVENTUAL ACTUALIZADO
        If (len(LTRIM(@F_nombre1)) < 2) Or (isnumeric(@F_nombre1) = 1) Or (len(LTRIM(@F_apellido3)) < 2) Or (isnumeric(@F_apellido3) = 1) Or (len(LTRIM(@F_apellido2)) < 2 and LEN(LTRIM(@F_apellido2)) > 0) Or (isnumeric(@F_apellido2) = 1 and LEN(LTRIM(@F_apellido2)) > 0) Or (len(LTRIM(@F_identificacion)) < 4 ) Or (len(LTRIM(@F_telefono_eventual)) < = 3) Or (isnumeric(@F_telefono_eventual) = 0) OR (@F_ok = 0)
        begin
            set @O_error_msg = 'Debe Actualizar los datos del cliente eventual (Nombre, Apellido Paterno, Apellido Materno, Identificación y Teléfono)'
            goto linea_error
        end

           -------------------------------------------------------------------------------------------
           --------  VERIFICACION SI EL CLIENTE EVENTUAL TIENE REGISTRADO EL NIVEL DE INGRESO    -----
           -------------------------------------------------------------------------------------------
            if exists(select 1 from climst_eventual
                              where cliente=@I_cliente
                                and indicador='A')
                        begin
                            select @F_nivel_ingreso = ingreso
                                                  from climst_adicionalEventual
                                                where cliente   = @I_cliente
                                                  and indicador = 'A'
                                                  and DATEDIFF(day,fecha_proceso,@F_fecha_proceso)<365----verifica que la ultima modificacion de nivel de ingreso no sea mayor a un año

                            if isnull(@F_nivel_ingreso,0)=0
                            begin
                               SET @O_error_msg = 'Para continuar con la operación debe actualizar el NIVEL DE INGRESO del cliente eventual'
                               GOTO linea_error
                            end

                        end---fin  si es eventual
    end
    -- VALIDAR LA CIUDAD DEL DOCUMENTO
    If @F_tipo_identificacion = 1
    begin
        set @F_sigla_ciudad_emision = dbo.fn_cli_identificacion(@F_identificacion, 1, 3)
        select @I_ciudad_exp_doc = descripcion from pam_ciudad where pais = @F_pais_emitido_ident and sigla = @F_sigla_ciudad_emision AND indicador = 'A'

        select @F_error = @@error, @F_rowcount = @@rowcount
        If @F_error <> 0 Or @F_rowcount > 1
        begin
            set @O_error_msg = concat('Error al leer pam_ciudad .error = ', rtrim(cast(@F_error as char(7))), ',rowcount = ', rtrim(cast(@F_rowcount as char(7))))
            GOTO linea_Error
        end

        If @F_rowcount = 0
        begin
            set @O_error_msg = concat('No existe la ciudad del documento', STR(@F_pais_emitido_ident), @F_sigla_ciudad_emision)
            goto linea_error
        end
    end

    iF @F_tipo_identificacion in(99, 18)--PASAPORTE
    BEGIN
        If LEN(LTRIM(rtrim(isnull(@I_ciudad_exp_doc, '')))) < = 0
        begin
            set @O_error_msg = 'Ingresar la ciudad de emisión del documento..'
            goto linea_error
        end
    END
    Select linea_texto = concat('I@I_ciudad_exp_docÆ', ltrim(@I_ciudad_exp_doc))

    -- VERIFICAR LAS COINCIDENCIAS DEL BENEFICIARIO CON EL CLIENTE EVENTUAL O CLIENTE FIJO
    if len(isnull(@F_apellido_beneficiario, '')) > 0
    begin
        select @f_nomb_concide = dbo.fn_comparar_palabras(ltrim(rtrim(@F_nombre_beneficiario)), @F_nombre1, 1),
            @f_ap_concide = dbo.fn_comparar_palabras(ltrim(rtrim(@F_apellido_beneficiario)), @F_apellido1, 1)
    end
    else
    begin
        select @f_nomb_concide = dbo.fn_comparar_palabras(ltrim(rtrim(@F_nombre_beneficiario)), rtrim(ltrim(concat(rtrim(@F_nombre1), ' ', ltrim(@F_apellido1)))), 2)
    end

    If @f_nomb_concide <> 0 OR @f_ap_concide <> 0
    Begin
        set @O_error_msg = 'El nombre del Beneficiario no Coincide con el nombre del Cliente/Eventual.'
        goto linea_error
    end

    If(select dpto from pam_ciudad where pais = 1 and LTRIM(RTRIM(departamento)) = LTRIM(RTRIM(@F_ciudad_beneficiario)) and indicador = 'A') <> @F_dpto_usuario and @F_origen_carga = 1 and @I_origen in(0)
    begin
        set @O_error_msg = 'Solo se puede entregar la Remesa en la ciudad indicada por el Remitente. Verificar con el Encargado Operativo'
        goto linea_error
    end
    -- VALIDAR LA FECHA DE VENCIMIENTO
    IF LEN(ISNULL(@I_fecha_venc_doc, '')) < = 0
    Begin
        set @O_error_msg = 'Ingresar la fecha de vencimiento del documento presentado'
        goto linea_error
    end

    IF ISDATE(@I_fecha_venc_doc) <> 1 Or LEN(ISNULL(@I_fecha_venc_doc, '')) <> 10
    Begin
        set @O_error_msg = 'La fecha de vencimiento ingresado no se encuentra en Formato DD/MM/AAAA'
        goto linea_error
    end
    --validar que que la fecha no se encuentre vencida
    IF ISDATE(@I_fecha_venc_doc) = 1 and cast(@I_fecha_venc_doc as smalldatetime) < @F_fecha_proceso
    Begin
        set @O_error_msg = 'El documento de identidad está vencido. Verifique la fecha de vencimiento'
        goto linea_error
    end

    Set @O_fecha_venc_identi = CAST(@I_fecha_venc_doc as Smalldatetime)
    -- MOSTRAR EL IMPORTE
    set @F_importe_total = 0--@F_Importe
    set @F_moneda = @F_moneda_pago

    IF @F_moneda = 4
    begin
        EXEC @F_error_exec = convertir_moneda_GLB
            @I_fecha_proceso = @F_fecha_proceso,
            @I_oficina = @F_oficina,
            @I_recibe = 0,--0 = entrega 1 = recibe
            @I_moneda_entrada = @F_moneda_pago,
            @O_moneda_salida = 1, --bs
            @I_importe_origen = @F_Importe,
            @O_importe_convertido = @F_Importe output,
            @O_error_msg = @O_error_msg output

        IF @@error <> 0
        BEGIN
            SET @O_error_msg = 'Error al ejecutar convertir_moneda_GLB.'
            GOTO LINEA_error
        END
        IF @F_error_exec <> 0 GOTO LINEA_error
        set @F_moneda = 1
    end
    -- SI NO SE COLOCA NADA EN EFECTIVO
    If @I_efectivo = 0 and @I_sistema_via = 0
    Begin
        set @I_efectivo = @F_Importe
    end
    If @I_efectivo > 0 and @I_efectivo > @F_importe
    BEGIN
        SET @O_error_msg = concat('El monto en efectivo es mayor al monto de la remesa a entregar, Verifique.', str(@I_efectivo, 13, 2), str(@F_importe, 13, 2))
        GOTO LINEA_error
    END

    If @I_efectivo > = 0
        set @F_importe_total += @I_efectivo
    else
        set @F_importe_total = @F_Importe
    -- Validacion de la forma de pago
    If isnull(@I_sistema_via, 0) = 0 and @F_forma_pago <> 30--Si se quiere entergar la remesa en efectivo -- Efectivo
    begin
        set @O_error_msg = 'No se puede realizar el pago o entrega de remesa en efectivo, porque se encuentra configurada para otra vía'
        goto linea_error
    end

    IF @I_sistema_via > 0-- Si Ingresa el Sistema
    Begin
        If not exists(select 1 from pam_sistema where sistema = @I_sistema_via and indicador = 'A')
        BEGIN
            SET @O_error_msg = 'El sistema de la vía de Pago No existe'
            GOTO LINEA_error
        END
        If @F_importe_total > = @F_importe
        BEGIN
            SET @O_error_msg = 'No hay saldo para la vía de Pago seleccionada, Verifique.'
            GOTO LINEA_error
        END
        If ISNULL(@I_cuenta_via, 0) < = 0
        BEGIN
            SET @O_error_msg = 'Ingresar La cuenta de la vía de Pago de la Remesa'
            GOTO LINEA_error
        END

        if @I_sistema_via = 200
        begin
            If @F_forma_pago not in(200, 30)-- que no sea CAH
            begin
                set @O_error_msg = 'No se puede realizar el pago o entrega de remesa a caja de ahorro, porque se encuentra configurada para otra vía.'
                goto linea_error
            end

            If @I_cuenta_via <> @F_cuenta and @F_forma_pago not in(200,30)-- que no sea CAH
            begin
                set @O_error_msg = 'La caja de ahorro seleccionada es diferente a la caja de ahorro que tiene la remesa configurada'
                goto linea_error
            end
            if @F_cli_eventual = 1-- Si es Eventual
            begin
                set @O_error_msg = 'El cliente Eventual no tiene ningún producto en la entidad'--'El nombre del Beneficiario no Coincide con el nombre del Cliente principal de la caja de ahorro'
                goto linea_error
            end

            select top 1
                @F_cliente_cah = c.nombre_ful,
                @F_cliente_cah2 = c.cliente,
                @F_moneda_via = a.moneda,
                @F_nombre1 = ltrim(d.nombre1),
                @F_apellido1 = ltrim(d.apellido1)+' '+ltrim(d.apellido2)+' '+ltrim(d.apellido3)
            from cahmst_maestro as a,
                climst_clioper as b,
                climst_cliente as c,
                climst_persona as d
            where a.nro_cuenta = @I_cuenta_via
                and a.indicador = 'A'
                and a.nro_cuenta = b.nro_cuenta
                and b.sistema = 200
                and b.fecha_proceso_hasta = '01-01-2050'
                and b.sec = 1
                and b.cliente = c.cliente
                and c.fecha_proceso_hasta = '01-01-2050'
                and c.indicador = 'A'
                and d.cliente = c.cliente
                and d.fecha_proceso_hasta = '01-01-2050'
                and d.indicador = 'A'
            select @F_error = @@error, @F_rowcount = @@rowcount
            If @F_error <> 0 Or @F_rowcount >1
            begin
                set @O_error_msg = concat('Error al leer cahmst_maestro .error = ', rtrim(cast(@F_error as char(7))), ',rowcount = ', rtrim(cast(@F_rowcount as char(7))))
                GOTO linea_Error
            end
            If @F_rowcount = 0
            begin
                set @O_error_msg = 'La caja de Ahorro no existe, Verifique'
                goto linea_error
            end
            -- VERIFICAR LAS COINCIDENCIAS DEL BENEFICIARIO CON EL CLIENTE EVENTUAL O CLIENTE FIJO
            If @F_cliente_cah2 <> @I_cliente
            Begin
                set @O_error_msg = 'El nombre del Beneficiario no Coincide con el nombre del Cliente principal de la caja de ahorro'
                goto linea_error
            end

            SET @F_importe_ope = @F_importe - @F_importe_total
            --SI HAY QUE REALIZAR COMPRA VENTA
            SET @F_imp_conv_cah = 0
            If @F_moneda_via <> @F_moneda
            begin
                EXEC @F_error_exec = convertir_moneda_GLB
                    @I_fecha_proceso = @F_fecha_proceso,
                    @I_oficina = @F_oficina,
                    @I_recibe = 1,  --0 = entrega 1 = recibe
                    @I_moneda_entrada = @F_moneda,
                    @O_moneda_salida = @F_moneda_via,
                    @I_importe_origen = @F_importe_ope,
                    @O_importe_convertido = @F_imp_conv_cah  output,
                    @O_error_msg = @O_error_msg output

                IF @@error<>0
                BEGIN
                    SET @O_error_msg = 'Error al ejecutar convertir_moneda_GLB.'
                    GOTO LINEA_error
                END
                IF @F_error_exec<>0  GOTO LINEA_error
            end
            set @F_importe_total += @F_importe_ope
        end --fin del if @F_via_sistema = 200 begin
        -------------------------- C U E N T A  C O R R I E N T E -------------------------------------
        If @I_sistema_via = 100 -- CCTA --100 cct...
        begin
            set @O_error_msg = 'No está habilitado para este Sistema. Verifique...'
            goto LINEA_error
            If @F_forma_pago <> 100-- que no sea cct
            begin
                set @O_error_msg = 'No se puede realizar el pago o entrega de remesa a cuenta corriente, porque se encuentra configurada para otra vía'
                goto linea_error
            end
        end
        --------------------------    C O N T A B I L I D A D   ---------------------------------------
        if @I_sistema_via = 910
        begin
            set @O_error_msg = 'No está habilitado para este Sistema . Verifique...'
            GOTO LINEA_error
            If @F_forma_pago <> 100-- que no sea contabilidad
            begin
                set @O_error_msg = 'No se puede realizar el pago o entrega de remesa a cuenta contable, porque se encuentra configurada para otra vía'
                goto linea_error
            end
        end --fin del if @F_via_sistema = 910 begin
    end

    If @F_importe_total > @F_Importe
    BEGIN
        SET @O_error_msg = 'El importe total es Mayor al monto de la remesa o monto a Entregar, Verifique.'
        GOTO LINEA_error
    END

    If @F_importe_total < @F_Importe
    BEGIN
        SET @O_error_msg = 'El importe en efectivo es menor al importe total de la remesa, Verifique.'
        GOTO LINEA_error
    END
    -- OBTENER LA COMISION DE LA REMESA
    set @F_comision = dbo.fn_gir_calculo_comision(@F_fecha_proceso, @F_remesadora, @F_Importe, @F_moneda, 2, 1)--tipo_envio = 1 por recepcion
    -- Validar que la remesadora
    If isnull(@F_comision, -1) < 0
    BEGIN
        SET @O_error_msg = 'La remesa excede el rango parametrizado de comisiones. Consulte con el Área de Canales Complementarios.'
        GOTO LINEA_error
    END
    -- OBTENER EL IMPORTE SEGUN LA MONEDA CONSOLIDADO EN ESTE CASO ESTA EN DOLARES
    set @F_monto_total = isnull(dbo.fn_cambio_moneda(@F_moneda, @F_moneda_limite_cons, @F_Importe, @F_fecha_proceso, @f_contable, 0), 0)
    --set @F_monto_total += @F_comision
    -- A MONEDA SE DEBE CAMBIAR DE VARIABLE  CUANDO SOLO SE ACEPTA DOLAR
    set @F_moneda_aux = @F_moneda
    if @F_moneda_dep = 1
        set @F_moneda_aux = 2
    -- VALIDAR LOS LIMITES DE LA REMESADORA
    EXEC @F_error_exec = proc_gir_Validar_limite_remesadora
        @I_Origen = 0, -- 0 = validar 1 = retornar datos
        @I_fecha_proceso = @F_fecha_proceso,
        @I_remesadora = @F_remesadora,
        @I_tipo_limite = 1, -- Limite x pago de remesa
        @I_monto = @F_monto_total,
        @I_moneda = @F_moneda_limite_cons,
        @O_monto_pagado = @F_monto_pagado output,
        @O_error_msg = @O_error_msg output
    IF @@error <> 0
    BEGIN
        SET @O_error_msg = 'Error al ejecutar proc_gir_Validar_limite_remesadora.'
        GOTO LINEA_error
    END
    IF @F_error_exec <> 0 and @I_origen in(0, 3)-- si existe error -- si es por pantalla
        GOTO LINEA_error
    IF @F_error_exec <> 0 and @I_origen = 1-- y es llamado del automatico
    begin
        SET @O_error_msg = ''
        return 0
    end
    --------------------------------------------------------------------------------------------------
    -- Obtener el monto
    select @F_monto_limite = monto
    from pamlav_lavado
    where indicador = 'A'
        and fecha_proceso_hasta = '01-01-2050'
    --  MOSTRAR EL IMPORTE CUANDO SE HAYA TERMINADO DE VALIDAR EL LIMITE
    IF @I_origen in(0, 3)
    Begin
        Select linea_texto = concat('I@I_remesadoraÆ', ltrim(STR(@I_remesadora)))
        union all Select concat('D@I_remesadoraÆ', ltrim(@F_nombre_remesadora))
        union all Select concat('I@I_claveÆ', ltrim(@I_clave))
        Union all Select concat('I@I_nombre_beneficiarioÆ', ltrim(upper(@F_nombre_ful_Beneficiario)))
        Union all Select concat('I@F_importeÆ', ltrim(@F_importe))
        Union all Select concat('I@F_monedaÆ', isnull((select sigla from pam_moneda where moneda = @F_moneda_pago and indicador = 'A'), ''))
    End
    ---------------------------------------------------------------------------------------------------
    --  VALIDAR LIMITE POR MONTO PLAZO Y TRANSACCION
    ---------------------------------------------------------------------------------------------------
    EXEC @F_error_exec = proc_gir_Validar_limite_x_remesa
        @I_Origen = 0, -- 0 = validar 1 = retornar datos
        @I_fecha_proceso = @F_fecha_proceso,
        @I_remesadora = @F_remesadora,
        @I_monto = @F_monto_total,
        @I_moneda = @F_moneda,
        @I_cliente = @I_cliente,
        @I_tipo_envio = 1,
        @O_error_msg = @O_error_msg output
    IF @@error <> 0
    BEGIN
        SET @O_error_msg = 'Error al ejecutar proc_gir_Validar_limite_x_remesa.'
        GOTO LINEA_error
    END
    IF @F_error_exec<> 0 and @I_origen in(0, 3)-- si existe error -- si es por pantalla
        GOTO LINEA_error
    IF @F_error_exec <> 0 and @I_origen = 1-- si existe error -- y es llamado del automatico
    begin
        SET @O_error_msg = ''
        return 0
    end
    ----------------------------------------------------------------------------------------------
    -------- GUARDAR EN LA TABLA TEMPORAL EL DETALLE DE LA PANTALLA (aumentado por cristina)
    ----------------------------------------------------------------------------------------------
    set @F_comprobante_auxpcc = 0
    select @F_str_ciudad_bene = isnull(@F_str_ciudad_bene, ''),
        @F_pais_remitente = isnull(@F_pais_remitente, '') ,
        @F_sigla_pais_benef = 'BOL'

    exec @F_error_exec = proc_lav_guardarformulariopcc01
        @I_codigo_opcion = 19,
        @I_sistema = 801,
        @I_nro_cuenta = 0,
        @I_cliente = @I_cliente,
        @I_tipo_operacion = 1,--1.retiro   2.deposito
        @I_cadena_deposito = '',
        @I_importe_total = @I_efectivo,--@I_dolares, --en la moneda de la cuenta
        @I_moneda = @F_moneda,
        @I_tipo_transaccion = 1, -- 1 = efectvi 4.compra  5.venta
        @I_usuario = @F_usuario,
        @I_fecha_today = @F_fecha_desde_log,
        @I_fecha_proceso = @F_Fecha_Proceso,
        @I_origen_pantalla = 9, ---1.deposito 2.retiro, 3.compra y venta, 4.ingreso y egreso otros sect. 5.cartera 9 = remesa
        @I_agencia = @F_agencia,
        @I_cod_tran = 801,--pago de remesa
        @I_grabar = @I_grabar,
        @I_pais_str_origen = @F_pais_remitente,
        @I_ciudad_origen = @F_ciudad_remitente,
        @I_pais_str_dest = @F_sigla_pais_benef,
        @I_ciudad_destino = @F_ciudad_beneficiario,
        @O_comprobante = @F_comprobante_auxpcc output,
        @O_error_msg = @O_error_msg output
    IF @@error <> 0
    BEGIN
        SET @O_error_msg = 'Error al ejecutar proc_lav_guardarformulariopcc01.'
        GOTO linea_error
    END
    IF @F_error_exec <> 0 GOTO linea_error
    ---------------------------------------------------------------------------------------------------
    --  VALIDACION LAVADO DE DINERO  (REALIZA LA SUMATORIA DE TODO LO Q TIENE EL USUARIO PARA SOLICITAR FORMULARIO 1)
    ---------------------------------------------------------------------------------------------------
    set @F_comprobanteF1 = 0

    exec @F_error_exec = Validar_sumatoria_lav
        @I_codigo_opcion = 19,
        @I_cliente = @I_cliente,
        @O_comprobante = @F_comprobanteF1 OUTPUT,
        @I_tipo_operacion = 8, --1 = compra venta 2 = deposito ahorros 3 = retiro ahorros 4 = pago cajero 5 = ingreso egresos otros sectores 7 = cartera  8 = remesa
        @I_sistema = 801,
        @I_nro_cuenta = 0, -- no es obligatorio poner cero
        @I_moneda = @F_moneda, -- mda de la Remesa.
        @I_importe = @I_efectivo,
        @I_fecha_proceso = @F_fecha_proceso,
        @I_comp_solpcc01 = @F_comprobante_auxpcc,
        @O_error_msg = @O_error_msg output
    IF @@error<>0
    BEGIN
        SET @O_error_msg = 'Error al ejecutar Validar_sumatoria_lav.'
        GOTO linea_error
    END
    IF @F_error_exec <> 0 GOTO linea_error
    -- Validar si es cliente eventual
    /*****************   VALIDAR CAMPOS SI ES CLIENTE EVENTUAL    ********************/
    If @F_cli_eventual = 1 and @I_sistema_via in(0, 30) and dbo.fn_cambio_moneda(@F_moneda, 2,@F_importe, @F_fecha_proceso, @F_contable, 0)<@F_monto_limite and dbo.fn_cambio_moneda(@F_moneda, 2,@F_importe, @F_fecha_proceso, @F_contable, 0) between @F_valor_minimo and @F_valor_maximo-- si es cliente Eventual-- Si es Efectivo -- si es menor al limite de PCC01
    Begin
        If LEN(ltrim(@I_destino)) < = 0
        BEGIN
            SET @O_error_msg = 'Para Cliente Eventual, debe ingresar el Destino de la transacción'
            GOTO linea_error
        END

        If LEN(ltrim(@I_destino)) < = 12--cantidad de caracter
        BEGIN
            SET @O_error_msg = 'El Destino de la transacción debe ser mayor a 12 caracteres'
            GOTO linea_error
        END

        If LEN(ltrim(@I_motivo)) < = 0
        BEGIN
            SET @O_error_msg = 'Para Cliente Eventual, debe ingresar el Motivo de la transacción'
            GOTO linea_error
        END

        If LEN(ltrim(@I_motivo))< = 12 --cantidad de caracter
        BEGIN
            SET @O_error_msg = 'El Motivo de la transacción debe ser mayor a 12 caracteres'
            GOTO linea_error
        END
    End
    If (@F_cli_eventual = 0) Or (@F_cli_eventual = 1 and @I_sistema_via in(0, 30) and dbo.fn_cambio_moneda(@F_moneda, 2, @F_importe, @F_fecha_proceso, @F_contable, 0) > = @F_monto_limite) Or (@F_cli_eventual = 1 and @I_sistema_via in(0, 30) and dbo.fn_cambio_moneda(@F_moneda, 2, @F_importe, @F_fecha_proceso, @F_contable, 0) < @F_valor_minimo)-- Si es cliente, no deben estar llenado los campos del motivo -- si es cliente eventual-- si es efectivo y si el monto es > = monto limite no deben estar llenados los campos-- si es cliente eventual-- si es efectivo y si el monto es > = monto limite no deben estar llenados los campos
    begin
        If LEN(ltrim(@I_destino)) > 0 Or LEN(ltrim(@I_motivo)) > 0
        BEGIN
            If @F_cli_eventual = 0
                SET @O_error_msg = 'Para cliente no es necesario el llenado de los campos Destino y Motivo'
            else if @F_cli_eventual = 1
                SET @O_error_msg = 'No es necesario el llenado de los campos Destino y Motivo, cuando el monto supera al límite.'
            GOTO linea_error
        END
    end
    /********************************************************************************* */
    ---------------------------------------------------------------------------------------------
    -- ASIGNACION DE LOS DATOS A LAS LINEAS PARA MOSTRAR
    ---------------------------------------------------------------------------------------------
    set @F_linea1 = concat(SPACE(58), ' REMESADORA: ', ltrim(@F_nombre_remesadora))
    set @F_linea2 = concat('BENEFICIARIO: ', cast(ltrim(upper(@F_nombre_ful_Beneficiario))as char(35)), SPACE(9), ' REMITENTE : ', cast(LTRIM(upper(@F_nombre_ful_remitente))as CHAR(35)))
    set @F_linea3 = concat('CIUDAD      : ', cast(ltrim(upper(@F_ciudad_beneficiario))as char(30)), SPACE(14), ' PAIS      : ', cast(LTRIM(upper(@F_pais_remitente))as CHAR(30)))
    ---------------------------------------------------------------------------------------------
    --  MOSTRAR DATOS
    ---------------------------------------------------------------------------------------------
    select @F_linea = '', @F_linea4 = ''
    SET @F_sigla_mda = isnull((select sigla from pam_moneda where moneda = @F_moneda and indicador = 'A'), '')
    if @I_efectivo > 0
    begin
        set @F_linea = concat(SPACE(24), ltrim(str(@I_efectivo,13,2)), ' ', cast(@F_sigla_mda as CHAR(3)), ' ENTREGARA EL CAJERO')
    end

    If @I_cuenta_via > 0
    begin
        set @F_linea4 = concat(iif(@F_imp_conv_cah > 0, cast(concat(LTRIM(STR(@F_importe_ope ,13,2)), ' ', cast(@F_sigla_mda as CHAR(3))) as char(18)), SPACE(19)), SPACE(5), IIF(@F_imp_conv_cah > 0 , ltrim(str(@F_imp_conv_cah,13,2)), concat(ltrim(str(@F_importe_ope,13,2)), ' ', isnull((select sigla from pam_moneda where moneda = @F_moneda_via  and indicador = 'A'), ''))), ' SE DEPOSITARA EN LA ', CASE @I_sistema_via WHEN 200 THEN 'CAH' WHEN 100 THEN 'CCT' WHEN 910 THEN 'CTA' END)
    end

    If @I_origen in(0, 3)
    Begin
        Select linea_texto = concat('I@I_nro_docÆ', ltrim(@F_identificacion))
        Union all Select concat('I@I_tipo_docÆ', ltrim(rtrim(@F_tipo_ident)))
        Union all Select concat('I@I_ciudad_exp_docÆ', ltrim(rtrim(@I_ciudad_exp_doc)))
        Union all Select concat('I@I_fecha_venc_docÆ', ltrim(rtrim(@I_fecha_venc_doc)))
        Union all Select concat('I@I_efectivoÆ', ltrim(str(@I_efectivo,13,2)))
        Union all Select concat('D@I_efectivoÆ', (@F_linea))
        Union all Select concat('I@I_sistema_viaÆ', ltrim(str(@I_sistema_via)))
        Union all Select concat('I@I_cuenta_viaÆ', ltrim(str(@I_cuenta_via)))
        Union all Select concat('D@I_cuenta_viaÆ', ltrim(@F_linea4))
        Union all Select concat('I@I_clienteÆ', ltrim(str(@I_cliente)))
        Union all Select concat('D@I_clienteÆ', ltrim(rtrim(@F_nombre_full_cliente)))
        Union all Select concat('I@I_observacionesÆ', ltrim(@I_observaciones))
        Union all Select concat('I@I_destinoÆ', ltrim(@I_destino))
        Union all Select concat('I@I_motivoÆ', ltrim(@I_motivo))
        Union all Select concat('D@F_linea1Æ', @F_linea1)
        Union all Select concat('D@F_linea2Æ', ltrim(rtrim(@F_linea2)))
        Union all Select concat('D@F_linea3Æ', ltrim(rtrim(@F_linea3)))
    End
    --
    -- LEER EL SALDIA
    set @F_saldo_limite = 0
    set @F_comision_pendiente = 0

    EXEC @F_error_exec = proc_gir_leer_girmst_Saldia
        @I_remesadora = @F_remesadora ,
        @I_moneda = @F_moneda_aux,--@F_moneda,-- LEER LA MONEDA DE LA CUAL PERTENECE LA REMESADORA
        @O_saldo_deposito = @F_saldo_deposito output,
        @O_saldo_limite = @F_saldo_limite output,
        @O_comision_pendiente = @F_comision_pendiente output,
        @O_error_msg = @O_error_msg output
    IF @@error<>0
    BEGIN
        SET @O_error_msg = 'Error al ejecutar proc_gir_leer_girmst_Saldia.'
        GOTO LINEA_error
    END
    IF @F_error_exec<>0  GOTO LINEA_error
    -- Verificar que ingrese los montos  en las vias
    If @I_efectivo = 0 and @I_cuenta_via = 0
    BEGIN
        SET @O_error_msg = 'Ingresar el monto a las vías de entrega de remesa...'
        GOTO LINEA_error
    END

    If @I_cuenta_via > 0 and isnull(@I_sistema_via, 0) = 0
    BEGIN
        SET @O_error_msg = 'Ingresar El sistema para la cuenta Vía'
        GOTO LINEA_error
    END
    -- Verificar si es el Ultimo o el Primer registro de la Planilla cambiar estado del la cabecera

    set @F_cant_registro2 = iif(@I_origen = 3 , (select COUNT(1) from #girmst_detalle where Remesadora = @F_Remesadora and codigo_remesa = @F_codigo_remesa and estado = 2), (select COUNT(1) from girmst_detalle where Remesadora = @F_Remesadora and codigo_remesa = @F_codigo_remesa and fecha_proceso_hasta = '01-01-2050' and indicador in ('A', 'B') and estado = 2)) + 1-- solicitado

    set @F_estado_cabecera = 0
    set @F_estado_cabecera = iif(@F_cant_registro2 > = @F_cant_registro, 2, 1)-- 2 = entregada ; 1 = PENDIENTE
    ---------------------------------------------------------------------------------------------------
    --  DATOS DE LA REMESA
    ---------------------------------------------------------------------------------------------------
    select @O_tipo_iden = @F_tipo_ident,
        @O_nro_identificacion = @F_identificacion,
        @O_codigo_remesa = @F_codigo_remesa,
        @O_nro_registro = @F_nro_registro,
        @O_id_trn = @F_id_trn
    ---------------------------------------------------------------------------------------------------
    --  VALIDAR LIMITE DE DINERO EN EFECTIVO PERMITIDO PARA EL CAJERO                               --
    ---------------------------------------------------------------------------------------------------
    IF @I_efectivo > 0--efectivo en la mnda de la cta.
        exec @F_error_exec = Leer_CajMst_Autorizacion_CAJ
            @I_usuario = @F_usuario,
            @I_moneda = @F_moneda,
            @I_ingreso_egreso = 2, --egreso
            @I_fecha_proceso = @F_fecha_proceso,
            @I_importe = @I_efectivo,
            @I_grabar = @I_grabar,
            @O_error_msg = @O_error_msg output

    IF @@error <> 0
    BEGIN
        SET @O_error_msg = 'Error al ejecutar Leer_CajMst_Autorizacion_CAJ.'
        GOTO linea_error
    END
    IF @F_error_exec <> 0 GOTO linea_error
    ----------------------------------------------------------------------
    -- Validar que los usuario que no sean cajeros realicen el pago
    ----------------------------------------------------------------------
    if @I_origen in(0, 3) --cuando es por pantalla y tambien cuando es WS
    begin
        exec @F_error_exec = Validar_cajtrn_movcajas_CAJ
            @I_codtran = 1,
            @I_Cliente = @F_Usuario,
            @I_fecha_proceso = @F_fecha_proceso,
            @O_rowcount = @F_rowcount output,
            @O_tipo_usuario = @F_tipo_usuario output,
            @O_error_msg = @O_error_msg  output
        IF @@error<>0
        BEGIN
            SET @O_error_msg = 'Error al ejecutar Validar_cajtrn_movcajas_CAJ.'
            GOTO linea_error
        END
        IF @F_error_exec <> 0 GOTO linea_error
        if @F_rowcount = 0 and @F_tipo_usuario = 2-- cajero
        begin
            set @O_error_msg = 'El cajero no realizó apertura de caja.'
            GOTO linea_error
        end
        exec @F_error_exec = Validar_cajtrn_movcajas_CAJ
            @I_codtran = 7,
            @I_Cliente = @F_Usuario,
            @I_fecha_proceso = @F_fecha_proceso,
            @O_rowcount = @F_rowcount output,
            @O_tipo_usuario = @F_tipo_usuario output,
            @O_error_msg = @O_error_msg  output
        IF @@error<>0
        BEGIN
            SET @O_error_msg = 'Error al ejecutar Validar_cajtrn_movcajas_CAJ.'
            GOTO linea_error
        END
        IF @F_error_exec <> 0 GOTO linea_error

        If @F_rowcount > 0 and @F_tipo_usuario = 2
        begin
            set @O_error_msg = 'El cajero realizó cierre de caja'
            GOTO linea_error
        end
    end
    ---------------------------------------------------------------------------------------------
    -- ***************************  GRABAR ********************************
    ---------------------------------------------------------------------------------------------
    If @I_grabar = 1
    Begin---------------------------if @I_grabar = 1--------------------------------------------------------
        set @F_mensaje = 'Transacción Ok'
        --  LEE EL COMPROBANTE----------------------------------
        --------------------------------------------------------
        exec @F_error_exec = Leer_y_Act_PamCorrelativo
            @I_nombre = 'CompRemesa',
            @I_oficina = 1,
            @I_fecha_proceso = @F_fecha_proceso,
            @I_complejidad = 1, --0 = numero,  1 = numero+digitoVerificador+Año,  2 = numero+digitoVerificador, 3 = numero+Año, 4 = numero+agencia
            @O_correlativo = @F_comprobante OUTPUT,
            @O_error_msg = @O_error_msg output
        If @@error <> 0
        begin
            set @O_error_msg = 'Error al ejecutar Leer_y_Act_PamCorrelativo'
            goto linea_error
        end
        If @F_error_exec <> 0 goto linea_error

        select @F_total_pagar = 0,
            @F_x_pagar = 0,
            @F_pagado = 0,
            @F_sec = 0,
            @f_viaspago = '',
            @F_x_pagar = @F_importe

        While @F_total_pagar < @F_importe
        begin
            set @F_pagado = 0
            If @F_saldo_deposito > 0
            begin
                select @F_cod_tran = 801,--Retiro,
                    @F_concepto = 3,
                    @F_aux_saldo = @F_saldo_deposito - @F_x_pagar

                If @F_aux_saldo > = 0
                begin
                    select @F_saldo_deposito -= @F_x_pagar,
                        @F_pagado = @F_x_pagar,
                        @F_x_pagar = 0
                end
                If @F_aux_saldo < 0
                begin
                    select @F_x_pagar -= @F_saldo_deposito,
                        @F_pagado = @F_saldo_deposito,
                        @F_saldo_deposito = 0
                end
            end
            else If @F_x_pagar > 0-- retirar del limite de la remesadora
            begin
                select @F_cod_tran = 801,-- Presto
                    @F_concepto = 5,
                    @F_saldo_limite += @F_x_pagar,
                    @F_pagado = @F_x_pagar
            end
            -- Guardar el Pago
            If @F_pagado > 0
            Begin
                set @F_importe_sus = dbo.fn_cambio_moneda(@F_moneda, 2, @F_pagado, @F_fecha_proceso, @F_contable, 0)

                insert into girtrn_pagos_remesas
                select
                    comprobante = @F_comprobante ,
                    sec = @F_sec,
                    remesadora = @F_Remesadora,
                    codigo_remesa = ltrim(@F_codigo_remesa),
                    nro_registro = @F_nro_registro,
                    cod_tran = @F_cod_tran,
                    concepto = @F_concepto,
                    importe = @F_pagado,
                    moneda = @F_moneda,
                    importe_sus = @F_importe_sus,  -- importe convertido en moneda extrajera
                    dolar_cv = @F_contable,      -- dolar con la que se realizo la conversion
                    fecha_proceso = @F_fecha_proceso ,
                    indicador = 'A',
                    agencia = @F_agencia ,
                    cliente = @I_cliente,
                    observaciones = LTRIM(RTRIM(@I_observaciones)),
                    destino_trn = LTRIM(RTRIM(@I_destino)),
                    motivo_trn = LTRIM(RTRIM(@I_motivo)),
                    fecha_alta = @F_fecha_today,
                    usuario = @F_usuario,
                    fecha_rev = NULL,
                    usuario_rev = NULL,
                    motivo_rev = '',
                    subzona = @F_subzona,
                    tesorero = @F_tesorero,
                    tipo_envio = 1,
                    comsion_tarifa = 0,
                    otros = 0,
                    itf = 0
                If @@error<>0
                begin
                    set @O_error_msg = 'Error al ejecutar girtrn_pagos_remesas'
                    goto linea_error
                end
            end
            select @F_total_pagar += @F_pagado,
                @F_sec += 1
        end--Fin del While
        -- Insertar las vias de pagos
        If @I_efectivo  > 0
            set @f_viaspago = concat('30Æ0ÆÆ', ltrim(str(@I_efectivo, 13, 2)), 'Æ801Æ', ltrim(str(106)), 'Æ')
        If @I_sistema_via > 0
            set @f_viaspago = concat(@f_viaspago, '200Æ', ltrim(str(@I_cuenta_via )), 'ÆÆ', ltrim(str(@F_importe_ope, 13, 2)), 'Æ801Æ', ltrim(str(105)), 'Æ')

        exec @F_error_exec = proc_abmgir_guardar_viaspago_gir
            @I_aplicar_pago = @I_grabar,
            @I_comprobante = @F_comprobante,
            @I_oficina = @F_oficina,
            @I_agencia = @F_agencia,
            @I_fecha_proceso = @F_fecha_proceso,
            @I_fecha_today = @F_fecha_today,
            @I_remesadora = @F_remesadora,
            @I_moneda = @F_moneda,
            @I_usuario = @F_usuario,
            @I_viaspago = @F_viaspago,
            @I_proc_automatico = @F_proc_aut,
            @O_error_msg = @O_error_msg output
        if @@error <> 0
        begin
            set @O_error_msg = 'Error al ejecutar proc_abmgir_guardar_viaspago_gir.'
            GOTO LINEA_Error
        end
        if @F_error_exec <> 0 goto LINEA_Error
        -- Actualizar el detalle
        EXEC @F_error_exec = proc_abmgir_Insert_update_detalle_remesa
            @I_tipo = 1,
            @I_remesadora = @F_remesadora,
            @I_codigo_remesa = @F_codigo_remesa,
            @I_nro_registro = @F_nro_registro,
            @I_tipo_envio = 1,--Recepcion
            @I_fecha_pago = @F_fecha_proceso,
            @I_estado = 2,-- 2 = Entregado 1 = Pendiente 0 = solicitado 3 = Notificado 4 = Pago x remesadora,
            @I_estado_envio = 2,--para que no envie mensaje de celular
            @I_venc_doc_beneficiario = @I_fecha_venc_doc,--Actualiza la fecha de vencimeinto del Doc
            @I_ciudad_emision_doc = @I_ciudad_exp_doc,
            @I_ciudad_beneficiario = @F_ciudad_beneficiario, --@F_ciudad,
            @O_error_msg = @O_error_msg    output
        IF @@error<>0
        BEGIN
            SET @O_error_msg = 'Error al ejecutar proc_abmgir_Insert_update_detalle_remesa.'
            GOTO LINEA_error
        END
        IF @F_error_exec<>0  GOTO LINEA_error

        set @F_aplica = 0
        If @F_tipo_comision = 2 -- DIARIO EL PAGO SE LO HACE ESE MISMO RATO.
            set @F_aplica = 0
        -- Calcular la comision de la transaccion
        set @F_comision = dbo.fn_gir_calculo_comision(@F_fecha_proceso, @F_remesadora, @F_total_pagar, @F_moneda, @F_moneda, 1)--Recibidos
        -- REALIZA LA TRANSACCION DE LA COMISION
        --set @F_moneda_comis = case when @F_de
        EXEC @F_error_exec = proc_abmgir_Guardar_comision
            @I_tipo_envio = 1, --Recepcion
            @I_fecha_proceso = @F_fecha_proceso,
            @I_fecha_today = @F_fecha_today,
            @I_comprobante = @F_comprobante,
            @I_monto_comision = @F_comision,
            @I_moneda = @F_moneda,
            @I_sec = @F_sec,-- la secuencia de la transacion
            @I_remesadora = @F_Remesadora,
            @I_codigo_remesa = @F_codigo_remesa,
            @I_nro_registro = @F_nro_registro,
            @I_oficina = @F_oficina,
            @I_agencia = @F_agencia,
            @I_cliente = @I_cliente,
            @I_observaciones = @I_observaciones,
            @I_destino = @I_destino,
            @I_motivo = @I_motivo,
            @I_usuario = @F_usuario,
            @I_subzona = @F_subzona,
            @I_tesorero = @F_tesorero,
            @O_saldo_deposito = @F_saldo_deposito output,
            @O_saldo_limite = @F_saldo_limite output,
            @O_comision_pendiente = @F_comision_pendiente output,
            @O_error_msg = @O_error_msg output
        IF @@error<>0
        BEGIN
            SET @O_error_msg = 'Error al ejecutar proc_abmgir_Guardar_comision.'
            GOTO LINEA_error
        END
        IF @F_error_exec <>0 GOTO LINEA_error
        EXEC @F_error_exec = proc_abmgir_Mantenimiento_Girmst_saldia
            @I_remesadora = @F_remesadora,
            @I_moneda = @F_moneda_aux,--@F_moneda,
            @I_fecha_proceso = @F_fecha_proceso,
            @I_saldo_deposito = @F_saldo_deposito ,
            @I_saldo_limite = @F_saldo_limite,
            @I_comision_pendiente = @F_comision_pendiente,
            @I_reversion = 0,
            @O_error_msg = @O_error_msg output
        IF @@error <> 0
        BEGIN
            SET @O_error_msg = 'Error al ejecutar proc_abmgir_Mantenimiento_Girmst_saldia.'
            GOTO LINEA_error
        END
        IF @F_error_exec <>0 GOTO LINEA_error
        If @F_estado_head <> @F_estado_cabecera
        begin
            exec @F_error_exec = proc_abmgir_Insert_update_head_remesa
                @I_remesadora = @F_remesadora,
                @I_codigo_remesa = @F_codigo_remesa,
                @I_estado = @F_estado_cabecera,
                @O_error_msg = @O_error_msg OUTPUT
            IF @@error <> 0
            begin
                set @O_error_msg = 'Error proc_abmgir_Insert_update_head_remesa '
                goto linea_Error
            end
            if @F_error_exec <> 0 goto linea_Error
        end
        ----------------------------------------------------------------------------------------------
        -------- PROCESO QUE REGISTRA LA TRANSACCIONAL PARA FORMULARIO PCC-01
        ----------------------------------------------------------------------------------------------
        select @F_comprobante_caja = comprobante
        from cajhed_caja
        where comprobante2 = @F_comprobante
            and sistema = 801
            and indicador = 'A'

        exec @F_error_exec = proc_lav_regtranscaja
            @I_codigo_opcion = 19,
            @I_comprobante = @F_comprobante_caja,
            @I_sistema = 30,
            @I_comprobante1 = @F_comprobante,
            @I_sistema1 = 801,
            @I_nro_cuenta = 0,
            @I_moneda = @F_moneda,
            @I_cod_tran = @F_cod_tran,
            @I_importe_ope = @I_efectivo,
            @I_cliente = @I_cliente,
            @I_usuario = @F_usuario,
            @I_fecha_proceso = @F_fecha_proceso,
            @I_agencia = @F_agencia,
            @O_error_msg = @O_error_msg OUTPUT

        IF @@error <> 0
        BEGIN
            SET @O_error_msg = 'Error al ejecutar proc_lav_regtranscaja.'
            GOTO linea_error
        END
        IF @F_error_exec <> 0 GOTO linea_error
        --SET @O_error_msg = 'registra'+str((select count(*) from lavtrn_trans_acumulada  where codigo_opcion = 19
        --                                       and indicador = 'A'
        --                                       and cliente = @I_cliente ))
        -- GOTO linea_error
        ----------------------------------------------------------------------------------------------
        -------- GUARDA EN LA TABLA lavmst_detalle F1 LOS IMPORTES ACUMULATIVOS DEL CLIENTE EN EL DIA
        ----------------------------------------------------------------------------------------------
        if @F_comprobanteF1 > 0
        begin
            exec @F_error_exec = AbmLav_Guardar_detalleF1
            @I_codigo_opcion = 19,
            @I_comprobanteF1 = @F_comprobanteF1,
            @I_comprobante = @F_comprobante_caja, --es el comprobante de DPF
            @I_cliente = @I_cliente,
            @I_fecha_proceso = @F_fecha_proceso,
            @I_tipo_modulo = 8,--1 = compra y venta; 2 = DPF  8 = remesa
            @I_importe_sus = @F_importe_sus,--
            @I_agencia = @F_agencia,            @I_moneda = @F_moneda,
            @I_dolar_cv = @F_contable,
            @I_sistema = 801,
            @O_error_msg = @O_error_msg OUTPUT

            IF @@error <> 0
            BEGIN
                SET @O_error_msg = 'Error al ejecutar AbmLav_Guardar_detalleF1.'
                GOTO linea_error
            END
            IF @F_error_exec <> 0 GOTO linea_error
            ----------------------------------------------------------------------------------------------
            ----- PROCESO QUE RESPONDE
            ----------------------------------------------------------------------------------------------
            exec @F_error_exec = proc_lav_responderpcc01
                @I_comprobanteF1 = @F_comprobanteF1,
                @I_comprobante_caja = @F_comprobante_caja,
                @I_codigo_opcion = 19,
                @I_fecha_proceso = @F_fecha_proceso,
                @I_cliente = @I_cliente,
                @O_error_msg = @O_error_msg OUTPUT
            IF @@error <> 0
            BEGIN
                SET @O_error_msg = 'Error al ejecutar proc_lav_responderpcc01.'
                GOTO linea_error
            END
            IF @F_error_exec <> 0 GOTO linea_error
        end
        --Se Debe Actualizar la tabla de TRN de Descarga para actualizar
        --Buscar si se encuentra en la tabla
        If exists(select 1 from girmst_descarga_webservices where remesadora = @I_remesadora and nro_giro = @F_nro_giro and indicador = 'A')
        Begin
            set @F_metodo = 'Notificar'
            --Generar el XML de respuestas
            Exec @F_error_exec = proc_gir_input_remesadora
                @I_remesadora = @I_remesadora,
                @I_clave_remesa = @I_clave,
                @I_nro_giro = @F_Nro_giro,--'RECIBIDO',
                @I_id_trn = @F_id_trn,--cuando es por ventanilla
                @I_metodo = @F_metodo,
                @I_nomb_usuario = @F_nombcorto,
                @I_orden = 'PAGADO',
                @I_moneda = @F_moneda,
                @I_monto = @F_importe,
                @I_fecha_today = @F_fecha_today,--Fecha de pago
                @I_date_time_UTC = @O_date_time_UTC,
                @I_date_time_local = @O_date_time_local,
                @I_fecha_venc_identi = @I_fecha_venc_doc,
                @I_nro_identificacion = @F_identificacion,
                @I_tipo_iden = @F_tipo_ident,
                @I_tipo_ident_cli = @F_tipo_ident_cli,
                @I_pais_emision_ident = @F_pais_emitido_ident,
                @I_ciudad_emision_doc = @I_ciudad_exp_doc,
                @I_fecha_nac = @F_fecha_nac,
                @I_subzona = @F_subzona,
                @O_cuerpo_xml = @O_cuerpo_xml output,
                @O_string_xml = @O_string_xml output,
                @O_head_ini_xml = @O_head_ini_xml output,
                @O_head_fin_xml = @O_head_fin_xml output,
                @O_metodo_exe = @O_metodo_exe output,
                @O_error_msg = @O_error_msg output
            if @@error <> 0
            begin
                set @O_error_msg = concat('Error al ejecutar proc_gir_input_remesadora. error = ', rtrim(cast(@@error as char(7))))
                GOTO linea_error
            end
            if @F_error_exec <> 0 goto linea_error
            ------------------------------------------------------------------------
            ---  SI SE ESTA PAGANDO POR EL AUTOMATICO VERIFICAR EN LA TABLA DE TRN
            ------------------------------------------------------------------------
            If @F_origen_carga in(3)--Cuando es cargado por el automatico
            Begin
                -- Actaulizar el estado y el cuerpo del XML
                Exec @F_error_exec = proc_gir_Insert_update_girmst_descarga_webservices
                    @I_tipo = 1, --0 = nuevo 1 = actualizar
                    @I_Remesadora = @I_remesadora,
                    @I_nro_giro = @F_Nro_giro,
                    @I_Clave_trn = @I_clave,
                    @I_concepto = 1,--descarga de remesa
                    @I_fecha_notif_ws = '01-01-1900',--Actualizar
                    @I_estado_notific_ws = 0,--Estado notifiacado
                    @I_estado_remesa = 3,--Pagado
                    @I_cuerpo_xml = @O_cuerpo_xml,
                    @O_error_msg = @O_error_msg output
                if @@error <> 0
                begin
                    set @O_error_msg = concat('Error al ejecutar proc_gir_Insert_update_girmst_descarga_webservices. error = ', rtrim(cast(@@error as char(7))))
                    GOTO linea_error
                end
                if @F_error_exec <> 0 goto linea_error
            End--@F_origen_carga in(3)--Cuando es cargado por el automatico
        End
    End  ------if @I_grabar = 1
    -----------------------------------------------------------------------------------
    -- Actualizar si es WS la tabla de las descargas
    -----------------------------------------------------------------------------------
    If ((@I_origen = 1 and @I_sistema_via = 200 and @O_tipo_conexion = 3 and @I_grabar = 1) Or (@I_grabar = 0 and @I_origen in(0, 3) and @O_tipo_conexion = 3))
    Begin
        ------------------------------------------------------------------------
        ---  SI SE ESTA PAGANDO POR EL AUTOMATICO VERIFICAR EN LA TABLA DE TRN
        ------------------------------------------------------------------------
        set @F_existe = ''
        set @F_rowcount = 0
        If (@I_origen in(1, 0) and @I_sistema_via in(200, 0))--si se esta pagando por el automatico
        Begin
            --Buscar si se encuentra en la tabla
            select @F_existe = Nro_giro
            from girmst_descarga_webservices
            where remesadora = @I_remesadora
                and nro_giro = @F_nro_giro
                and indicador = 'A'
            Select @F_rowcount = @@ROWCOUNT,
                @F_error = @@ERROR,
                @F_existe = ISNULL(@F_existe, '')
        End--(@I_origen = 1  and @I_sistema_via = 200)

        select
            @F_usr_webservice = usr_webservice,
            @F_pasw_webservice = isnull(dbo.fn_Desencript_clave(pasw_webservice), ''),
            @F_url_dmz = url_webservice,
            @F_url_externa = url_ws_remesadora
        from girmst_webservice_remesadora
        where remesadora = @F_Remesadora
            and fecha_proceso_hasta = '01/01/2050'
        select @F_rowcount = @@ROWCOUNT
        If @F_rowcount <> 1
            select @F_usr_webservice = '',
                @F_pasw_webservice = '',
                @F_url_dmz = '',
                @F_url_externa = ''

        set @F_metodo =
            case
                when @I_origen In(0, 3) then 'Confirmar_Pago'
                when @I_origen = 1 and @F_rowcount = 1 then 'Notificar'
                when @I_origen = 0 and @F_rowcount = 1 then 'Notificar'
                else ''
            end
        --Generar el XML de respuestas
        Exec @F_error_exec = proc_gir_input_remesadora
            @I_remesadora = @I_remesadora,
            ----------------------------------------
            @I_usr_webservice = @F_usr_webservice,
            @I_pasw_webservice = @F_pasw_webservice,
            @I_url_dmz = @F_url_dmz,
            @I_url_externa = @F_url_externa,
            ----------------------------------------
            @I_clave_remesa = @I_clave,
            @I_nro_giro = @F_Nro_giro,--'RECIBIDO',
            @I_id_trn = @F_id_trn,--cuando es por ventanilla
            @I_metodo = @F_metodo,
            @I_nomb_usuario = @F_nombcorto,
            @I_orden = 'PAGADO',
            @I_moneda = @F_moneda,
            @I_monto = @F_importe,
            @I_fecha_today = @F_fecha_today,--Fecha de pago
            @I_date_time_UTC = @O_date_time_UTC,
            @I_date_time_local = @O_date_time_local,
            @I_fecha_venc_identi = @I_fecha_venc_doc,
            @I_nro_identificacion = @F_identificacion,
            @I_tipo_iden = @F_tipo_ident,
            @I_tipo_ident_cli = @F_tipo_ident_cli,
            @I_pais_emision_ident = @F_pais_emitido_ident,
            @I_ciudad_emision_doc = @I_ciudad_exp_doc,
            @I_fecha_nac = @F_fecha_nac,
            @I_subzona = @F_subzona,
            @O_cuerpo_xml = @O_cuerpo_xml output,
            @O_string_xml = @O_string_xml output,
            @O_head_ini_xml = @O_head_ini_xml output,
            @O_head_fin_xml = @O_head_fin_xml output,
            @O_metodo_exe = @O_metodo_exe output,
            @O_error_msg = @O_error_msg output

        if @@error <> 0
        begin
            set @O_error_msg = CONCAT('Error en ejecutar proc_gir_input_remesadora. error = ', rtrim(cast(@@error as char(7))))
            GOTO linea_error
        end
        if @F_error_exec <> 0 goto linea_error
        ------------------------------------------------------------------------
        ---  PROCESO PARA MODIFICAR PAIS Y CIUDAD DE LAVADO DE DINERO
        ------------------------------------------------------------------------
        if isnull(@F_comprobanteF1, 0) > 0
        begin
            Exec @F_error_exec = dbo.proc_lav_registro_remesa
                @I_remesadora           = @I_remesadora,
                @I_clave_remesa         = @I_clave,
                @I_cliente              = @I_cliente,
                @I_monto                = @F_Importe,
                @I_nro_cuenta           = @F_cuenta,
                @I_moneda_cuenta        = @F_moneda_pago,
                @I_comprobante_pcc01    = @F_comprobanteF1,
                @I_origen               = 1, --Donde 1 = pago de remesa,  2 = envio de remesa
                @O_error_msg            = @O_error_msg    output

            if @@error <> 0
            begin
                set @O_error_msg = CONCAT('Error en ejecutar proc_lav_registro_remesa. error = ', rtrim(cast(@@error as char(7))))
                GOTO linea_error
            end

            if @F_error_exec <> 0
                goto linea_error
        end
        -- ------------------------------------------------------------------------
        -- ---  SI SE ESTA PAGANDO POR EL AUTOMATICO VERIFICAR EN LA TABLA DE TRN
        -- ------------------------------------------------------------------------
    End
    set @O_comprobante = @F_comprobante
    ----------------------------------------------------------------------------------------------
    --------                            FIN       GRABAMOS                                --------
    ----------------------------------------------------------------------------------------------
    salir_procedimiento:
    begin
        if  @@NESTLEVEL = 1
        begin
            if @I_grabar = 1
                commit tran

            select linea_texto = CONCAT('M', @F_mensaje)

            iF @F_comprobante > 0
            Begin
                select linea_texto = 'R'
                exec proc_repgir_imprimir_pago_remesas
                    @I_comprobante = @F_comprobante,
                    @I_reimpresion = 0

                if isnull(@F_comprobanteF1, 0) > 0
                begin
                    select linea_texto = 'R'
                    exec rep_formulario1_lavado
                        @I_comprobante = @F_comprobanteF1,
                        @I_reimpresion = 0,
                        @O_error_msg = ''
                end
            END
            ---------------------------------------------------------------------------------------------------
            --  Grabar log
            ---------------------------------------------------------------------------------------------------
            set @F_observacion_error = concat(
                    'Clave:', isnull(ltrim(cast(@I_clave as varchar(30))),'nulo'),
                    ' rem:', isnull(ltrim(cast(@I_remesadora as varchar(8))),'nulo'),
                    ' Cli:', isnull(ltrim(cast(@I_cliente as varchar(12))),'nulo'),
                    ' Efec:', isnull(cast(@I_efectivo as varchar(16)),'Nulo'),
                    ' sist:', isnull(cast(@I_sistema_via as varchar(8)),'Nulo'),
                    ' cta_via: ', isnull(cast(@I_cuenta_via as varchar(10)),'Nulo'),
                    ' observ: ', isnull(LTRIM(RTRIM(cast(@I_observaciones as varchar(50)))),'nulo'),
                    ' dest: ', isnull(LTRIM(RTRIM(cast(@I_destino as varchar(50)))),'nulo'),
                    ' motivo: ', isnull(LTRIM(RTRIM(cast(@I_motivo as varchar(50)))),'nulo')
                )
            exec Grabar_log_GLB
                @I_usuario = @F_usuario,
                @I_fecha_proceso = @F_fecha_proceso,
                @I_observacion_error = @F_observacion_error,
                @I_nombre_sp = 'proc_abmgir_Pago_entrega_remesa',--Nombre del procedimiento almacenado
                @I_fecha_desde = @F_fecha_desde_log,
                @I_tipo_log = 2,--1 = reporte, 2 = abm
                @I_es_error = 0,--0 = no es error, 1 = si es error
                @I_error_msg = @O_error_msg
        end
        return 0
    end
    LINEA_error:
    begin
        set @O_error_msg = concat('proc_abmgir_Pago_entrega_remesa.§', rtrim(isnull(@O_error_msg, 'nulo')))
        if  @@NESTLEVEL = 1
        begin
            if @I_grabar = 1
                rollback tran
            ---------------------------------------------------------------------------------------------------
            --  Grabar log
            ---------------------------------------------------------------------------------------------------
            set @F_observacion_error = concat(
                            'Clave:', isnull(ltrim(cast(@I_clave as varchar(30))),'nulo'),
                            ' rem:', isnull(ltrim(cast(@I_remesadora as varchar(8))),'nulo'),
                            ' Cli:', isnull(ltrim(cast(@I_cliente as varchar(12))),'nulo'),
                            ' Efec:', isnull(cast(@I_efectivo as varchar(16)),'Nulo'),
                            ' sist:', isnull(cast(@I_sistema_via as varchar(8)),'Nulo'),
                            ' cta_via: ', isnull(cast(@I_cuenta_via as varchar(10)),'Nulo'),
                            ' observ: ', isnull(LTRIM(RTRIM(cast(@I_observaciones as varchar(50)))),'nulo'),
                            ' dest: ', isnull(LTRIM(RTRIM(cast(@I_destino as varchar(50)))),'nulo'),
                            ' motivo: ', isnull(LTRIM(RTRIM(cast(@I_motivo as varchar(50)))),'nulo')
                        )
            exec Grabar_log_GLB
                @I_usuario = @F_usuario,
                @I_fecha_proceso = @F_fecha_proceso,
                @I_observacion_error = @F_observacion_error,
                @I_nombre_sp = 'proc_abmgir_Pago_entrega_remesa',--Nombre del procedimiento almacenado
                @I_fecha_desde = @F_fecha_desde_log,
                @I_tipo_log = 2,--1 = reporte, 2 = abm
                @I_es_error = 1,--0 = no es error, 1 = si es error
                @I_error_msg = @O_error_msg
        end
        if @@nestlevel = 1 raiserror(@O_error_msg, 16, -1)
        return -1
    end
END
GO