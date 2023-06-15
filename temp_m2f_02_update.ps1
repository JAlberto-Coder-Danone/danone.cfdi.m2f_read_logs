# ######################################################################################
# 03-05-2023 | PowerShell que actualiza la fecha de transformación de un idoc, partiendo de la lectura de los logs
#              powershell.exe -ExecutionPolicy Bypass -File ".\temp_m2f_02_update.ps1"           
# 11-05-2023 | Se actualiza filtro de archivos modificación en las últimas 24 horas
# 17-05-2023 | Se actualiza y se agregan carpetas de backup para revisar documentos que han pasado por el transoformer y temrinaron en la carpeta de BCK
# ######################################################################################

# Define la ruta de la carpeta de entrada y salida
$carpetaBNF = "\\WMXQROB029\e$\DCT\CFDI\QUEUES\IMQ\REPORTES\BNF"
$carpetaEDP = "\\WMXQROB029\e$\DCT\CFDI\QUEUES\IMQ\REPORTES\EDP"
$carpetaIPP = "\\WMXQROB029\e$\DCT\CFDI\QUEUES\IMQ\REPORTES\IPP"
$path_backup_BNF = "\\WMXQROB029\e$\DCT\CFDI\QUEUES\IMQ\BNF\BKP"
$path_backup_EDP = "\\WMXQROB029\e$\DCT\CFDI\QUEUES\IMQ\EDP\BKP"
$logFolder = "C:\Monitoring\Logs\"
$logName = "logs_m2f_02_update_$(Get-Date -Format 'yyyyMMdd').log"
$logPath = Join-Path $logFolder $logName
$errores = @()
# Parámetros de conexión a la base de datos
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
    try {
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
            $filasExistentes = 25000
            Write-Host ($archivo.FullName) 
            foreach ($linea in $contenidoArchivo -split '\r?\n') {
                $contador++;
                $contenido = ""
                
                if ($contador -gt $filasExistentes) {
                    if ($linea -match "(.*).idoc") {
                        $contenido = $matches[1]
                        $contenidoSplit = $contenido.Split("-")
                        $idocNumber = $contenidoSplit[-1] + ".idoc"
                        
                        $billing_doc = $contenido.Split("-")[0]
                        try {
                            if (($contador + 1) % 100 -eq 0) {
                                Write-Host ("Procesados: " + $contador)    
                            } 
                            Invoke-Sqlcmd -ServerInstance $serverName -Database $databaseName -Username $username -Password $password -Query "EXEC idoc_pm.usp_resources_m2f_02_update @name_idoc='$($idocNumber)',@billing_doc='$($billing_doc)',@rfc_emitter='$($rfcEmisor)';" -ErrorAction Stop
                        } catch {
                            Write-Host ("ERROR")
                            $errores += "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') Error al insertar registro, archivo $($archivo.Name) y idoc $($nameIdoc) sp: usp_resources_m2f_02_update `r`n"
                        }
                    }
                }
            }
            
            if (-not $errores) {
                $result = Invoke-Sqlcmd -ServerInstance $serverName -Database $databaseName -Username $username -Password $password -Query "EXEC idoc_pm.usp_resources_transformation_control_consult @file_name_resource='$($archivo.FullName)', @rows_number=$($lineasArchivo), @rows_processed=$($contador);"
            }
        } catch {
            $errores += "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') Error al procesar el archivo $($archivo.Name)`r`n"
        }
    }

    return $errores
}

function folderProcess($path, $rfcEmitter) {
    $errores = @()
    $username = textoDecriptar($username)
    $password = textoDecriptar($password)
    $lastExecution = Get-Date
    try {
        $result = Invoke-Sqlcmd -ServerInstance $serverName -Database $databaseName -Username $username -Password $password -Query "EXEC idoc_pm.usp_resources_transformation_control_consult @file_name_resource='$($path)',@rows_number=1;" -ErrorAction Stop
        if ($null -ne $result) {
            $lastExecution = $result.lastExecution
        }

        $formatoFecha = "yyyy/MM/dd HH:mm:ss"
        $fecha = [DateTime]::ParseExact($lastExecution, $formatoFecha, $null)

        $documentos = Get-ChildItem -Path $path | Where-Object {
            $_.LastWriteTime -ge $fecha -and -not $_.PSIsContainer
        }

        if ($documento.Count -gt 0) {
            foreach ($documento in $documentos) {
                try {
                    $idoc_name = $documento.Name
                    $fechaModificacion = $documento.LastWriteTime.ToString($formatoFecha)
                    
                    Invoke-Sqlcmd -ServerInstance $serverName -Database $databaseName -Username $username -Password $password -Query "EXEC idoc_pm.usp_resources_m2f_02_update @name_idoc='$($idoc_name)',@billing_doc=NULL,@rfc_emitter='$($rfcEmisor), @date_tranform='$($fechaModificacion)';" -ErrorAction Stop
                } catch {
                    Write-Host ("ERROR")
                    $errores += "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') Error al insertar registro, archivo $($archivo.Name) y idoc $($nameIdoc) sp: usp_resources_m2f_02_update `r`n"
                }
            }
        }
    } catch {
        $errores += "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') Error al procesar"
    }
    # if (-not $errores) {
    #     ## Aquí ya no estaría o si?
    #     $result = Invoke-Sqlcmd -ServerInstance $serverName -Database $databaseName -Username $username -Password $password -Query "EXEC idoc_pm.usp_resources_transformation_control_consult @file_name_resource='$($path)',@rows_number='1';" -ErrorAction Stop
    # }
    return $errores
}

try {
    # Test de conexión a BD
    $connectionString = "Server=$serverName;Database=$databaseName;User ID=$(textoDecriptar($username));Password=$(textoDecriptar($password));Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;"
    $connection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
    $connection.Open()
    $connection.Close()
} catch {
    $errores += "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') Error al procesar el archivo $($archivo.Name): $($_.Exception.Message)?"
    $connection.Close()    
    logEscribe(1)
}
Write-Output  "Comienza"
# $archivos = Get-ChildItem $carpetaBNF | Where-Object {$_.Extension -eq ".txt" -and $_.LastWriteTime -ge (Get-Date).AddHours(-24)}
# $errores += resourceProcesar $archivos 'BON9206241N5'
# $archivos = $null
# $archivos = Get-ChildItem $carpetaEDP | Where-Object {$_.Extension -eq ".txt" -and $_.LastWriteTime -ge (Get-Date).AddHours(-24)}
# $errores += resourceProcesar $archivos 'DME761202CP9' 
# $archivos = $null
# $archivos = Get-ChildItem $carpetaIPP | Where-Object {$_.Extension -eq ".txt" -and $_.LastWriteTime -ge (Get-Date).AddHours(-24)}
# $errores += resourceProcesar $archivos 'ESU001009LX0'
# $archivos = $null
# Verifica la existencia de errores para escribirlos
$errores += folderProcess $path_backup_BNF 'BON9206241N5'
$errores += folderProcess $path_backup_EDP 'DME761202CP9'
Write-Output "FIN"
logEscribe(1)
