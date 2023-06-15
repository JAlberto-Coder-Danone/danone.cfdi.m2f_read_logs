# ######################################################################################
# 03-05-2023 | PowerShell que realiza lectura de documentos tipo log, y los inserta en BD en el proceso m2f
#              powershell.exe -ExecutionPolicy Bypass -File ".\temp_m2f_01_insert.ps1"
# 11-05-2023 | Se actualiza filtro de archivos modificación en las últimas 24 horas
# ######################################################################################

# Para Bonafont
$carpetaBNFmX2 = "\\Wusashmxb002\PAEBSA\BNF\M2F\logs\mx2"
$carpetaBNFmX3 = "\\Wusashmxb002\PAEBSA\BNF\M2F\logs\mx3"
# Para Danone
$carpetaDDM01 = "\\Wusashmxb002\PAEBSA\DDM\M2F\logs\01"
$carpetaDDM02 = "\\Wusashmxb002\PAEBSA\DDM\M2F\logs\02"
$carpetaDDM03 = "\\Wusashmxb002\PAEBSA\DDM\M2F\logs\03"
$carpetaDDM04 = "\\Wusashmxb002\PAEBSA\DDM\M2F\logs\04"
#Log Folder
$logFolder = "C:\Monitoring\Logs\"
$logName = "logs_m2f_01_insert_$(Get-Date -Format 'yyyyMMdd').log"
$logPath = Join-Path $logFolder $logName
$errores = @()
# Parámetros de conexión BD
$serverName = "dan-mx-p-sql003-cfdi40.database.windows.net"
$databaseName = "mx-cfdi40-prod"
$username = "ny.dgej51.qspe.pxofs.vtfs"
$password = 'XJ:im3pusBSPE%2i4L9u'
# Funciones
Function textoDecriptar($Text) {
    try {
        $Chars = [char[]]$Text
        For ($i=0; $i -lt $Chars.Length; $i++) {
            $Chars[$i] = [char]([int]$Chars[$i] - 1)
        }
        return $Chars -join ''
    } catch {
        return ''
    }
}
Function textoEncriptar($Text) {
    $Chars = [char[]]$Text
    For ($i=0; $i -lt $Chars.Length; $i++) {
        $Chars[$i] = [char]([int]$Chars[$i] + 1)
    }
    return $Chars -join ''
}

function logEscribe($salir) {
    try{
        if ($errores.Count -gt 0) {
            if (!(Test-Path -Path $logFolder)) {
                New-Item -ItemType Directory -Path $logFolder | Out-Null
            }
            if (!(Test-Path -Path $logPath)) {
                New-Item -ItemType File -Path $logPath | Out-Null
            }
            $errores | Out-File -FilePath $logPath -Append -NoNewline
        }
    } catch {
        Write-Output "La carpeta de log, no es accesible"
    }
    
    Exit
}

function fechaHoraAEntero([string]$fechaHora) {
    $fecha, $hora = $fechaHora.Split(" ")
    $fechaSinSeparadores = $fecha.Substring(6,4) + $fecha.Substring(3,2) + $fecha.Substring(0,2)
    $horaSinSeparadores = $hora.Replace(":", "")
    return [long]($fechaSinSeparadores + $horaSinSeparadores)
}

function resourceProcesar($archivos, $rfcEmisor) {
    $errores = @()
    $username = textoDecriptar($username)
    $password = textoDecriptar($password)
    $filasExistentes = 0
    foreach ($archivo in ($archivos)) {
        $contador = 0
        try {
            $contenidoArchivo = Get-Content $archivo.FullName -Raw
            $lineasArchivo = ($contenidoArchivo -split '\r?\n').Count
            
            $result = Invoke-Sqlcmd -ServerInstance $serverName -Database $databaseName -Username $username -Password $password -Query "EXEC idoc_pm.usp_resources_transformation_control_consult @file_name_resource='$($archivo.FullName)', @rows_number=$($lineasArchivo);"
            if ($null -ne $result) {
                $filasExistentes = $result.rowsNumber
            }
            
            if ($filasExistentes -eq 0) {
                $filasExistentes = 0
            } else {
                $filasExistentes = $filasExistentes - 1
            }

            $filasExistentes = 0

            foreach ($linea in $contenidoArchivo -split '\r?\n') {
                if ($contador -gt $filasExistentes) {
                    
                    if ($linea -match "EDI_DC40") {
                        #FechayHora
                        $fecha = $linea.Substring(0,8)
                        $hora = $linea.Substring(10,6)  
                        $fechaHora = "{0}/{1}/{2} {3}:{4}:{5}" -f $fecha.Substring(0,4), $fecha.Substring(4,2), $fecha.Substring(6,2), $hora.Substring(0,2), $hora.Substring(2,2), $hora.Substring(4,2)
                        #NombreIdoc
                        $indice = $linea.IndexOf("EDI_DC40  ") + 10
                        $cadena = $linea.Substring($indice, 19)
                        $nameIdoc = "P07" + $cadena.Substring(0, 3) + "_" + $cadena.Substring(3, $cadena.Length -3) + ".idoc"
                        try {
                            Invoke-Sqlcmd -ServerInstance $serverName -Database $databaseName -Username $username -Password $password -Query "EXEC idoc_pm.usp_resources_m2f_01_insert @name='$($nameIdoc)', @register_at='$($fechaHora)', @rfc_emitter='$($rfcEmisor)';" -ErrorAction Stop
                        } catch {
                            $errores += "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') Error al insertar registro, archivo $($archivo.Name) y idoc $($nameIdoc) sp: usp_resources_m2f_01_insert `r`n"
                        }
                    }
                }

                $contador++;
            }

            if (-not $errores) {
                $result = Invoke-Sqlcmd -ServerInstance $serverName -Database $databaseName -Username $username -Password $password -Query "EXEC idoc_pm.usp_resources_transformation_control_consult @file_name_resource='$($archivo.FullName)', @rows_number=$($lineasArchivo), @rows_processed=$($contador);"
            }
        } catch {
            $errores += "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') Error al procesar el archivo no se proceso ninguna línea $($archivo.Name)`r`n"
        }
    }
    return $errores
}

try {    
    # Test de conexión a BD
    $connectionString = "Server=$serverName;Database=$databaseName;User ID=$(textoDecriptar($username));Password=$(textoDecriptar($password));Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;"
    $connection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
    $connection.Open()
    $connection.Close()
} catch {
    $errores += "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') Error en conexión a BD $($archivo.Name): $($_.Exception.Message)?"
    $connection.Close()    
    logEscribe(1)
}
# Bonafon
$archivos = Get-ChildItem $carpetaBNFmX2 | Where-Object {$_.Extension -eq ".txt" -and $_.LastWriteTime -ge (Get-Date).AddHours(-24)}
$errores += resourceProcesar $archivos 'BON9206241N5' 
$archivos = $null
$archivos = Get-ChildItem $carpetaBNFmX3 | Where-Object {$_.Extension -eq ".txt" -and $_.LastWriteTime -ge (Get-Date).AddHours(-24)}
$errores += resourceProcesar $archivos 'BON9206241N5'
$archivos = $null
# Danone
$archivos = Get-ChildItem $carpetaDDM01 | Where-Object {$_.Extension -eq ".txt" -and $_.LastWriteTime -ge (Get-Date).AddHours(-24)}
$errores += resourceProcesar $archivos 'DME761202CP9' 
$archivos = $null
$archivos = Get-ChildItem $carpetaDDM02 | Where-Object {$_.Extension -eq ".txt" -and $_.LastWriteTime -ge (Get-Date).AddHours(-24)}
$errores += resourceProcesar $archivos 'DME761202CP9'
$archivos = $null
$archivos = Get-ChildItem $carpetaDDM03 | Where-Object {$_.Extension -eq ".txt" -and $_.LastWriteTime -ge (Get-Date).AddHours(-24)}
$errores += resourceProcesar $archivos 'DME761202CP9' 
$archivos = $null
$archivos = Get-ChildItem $carpetaDDM04 | Where-Object {$_.Extension -eq ".txt" -and $_.LastWriteTime -ge (Get-Date).AddHours(-24)}
$errores += resourceProcesar $archivos 'DME761202CP9' 
$archivos = $null
# Verifica la existencia de errores para escribirlos
Write-Output "FIN"
logEscribe(1)
