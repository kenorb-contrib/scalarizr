param (
    [string]$url = $(throw "Specify package URL to install")
)

$debugPreference = "continue"
$errorActionPreference = "stop"


$installDir = "$($env:PROGRAMFILES)\Scalarizr"
$uninstallRegKey = "hklm:\Software\Microsoft\Windows\CurrentVersion\Uninstall\Scalarizr"
$logDir = "$installDir\var\log"
$runDir = "$installDir\var\run"
$logFile = "$logDir\scalarizr_update_win.log"
$statusFile = "$installDir\etc\private.d\update_win.status"
$backupCreatedLock = "$runDir\backup.created"
$dirsToBackup = @("src", "Python27")
$servicesToOperate = @("ScalrUpdClient", "Scalarizr")
$installedVersion = ""
$state = "in-progress"
$prevState = $null


function log {
param (
    $message
)
    write-debug "$(get-date -format s) -  $message"
}


function tmpName {
param (
    $suffix
)
    return [system.io.path]::getTempPath() + [system.guid]::newGuid().toString() + $suffix
}


function downloadFile {
param (
    $url
)
    $wc = new-object system.net.WebClient
    $dst = tmpName -suffix $([system.io.path]::getExtension($url))
    log "Downloading $url to $dst"
    $wc.downloadFile($url, $dst)
    return $dst
}


function runInstaller {
param (
    $fileName
)
    log "Starting installer"
    $proc = start-process -wait -noNewWindow $fileName /S
    if ($proc.exitCode) {
        throw "Installer $(split-path -leaf $fileName) exited with code: $($proc.ExitCode)"
    }
    log "Installer completed"
    sleep 2  # Give them time to think
}


function extractZipFile {
param (
    $fileName,
    $destDir
)
    $FOF_NOCONFIRMATION = 0x10

    log "Extracting ZIP $fileName to directory $destDir"
    $shell = new-object -com shell.application
    $source = $shell.namespace($fileName)
    if (-not (test-path $destDir)) {
        new-item $destDir -type directory
    }
    $dest = $shell.namespace($destDir)
    $dest.copyHere($source.items(), $FOF_NOCONFIRMATION)
}


function acceptHandleUtilEula {
    $path = "hkcu:\software\sysinternals\handle"
    if (-not (test-path $path)) {
        new-item $(split-path $path -parent)
        new-item $path
    }
    set-itemproperty $path "EulaAccepted" "1"
}


function ensureHandleUtil {
    if (-not (get-command handle -errorAction silentlyContinue)) {
        log "Installing Sysinternals Handle (By Mark Russinovich)"
        $path = downloadFile -url "http://download.sysinternals.com/files/Handle.zip"
        try {
            extractZipFile -fileName $path -destDir $env:WINDIR
            acceptHandleUtilEula    
        } finally {
            remove-item $path
        }
    }
}


function releaseHandles {
param (
    $path
)
    log "Releasing handles for: $path"
    ensureHandleUtil
    handle $path | foreach { 
        if ($_ -match "[^\s]+\s+pid:\s(\d+)\s+type:\s[^\s]+\s+([^:]+):") { 
            log $_
            $pid_ = $matches[1]; 
            $hnd = $matches[2]; 
            handle -c $hnd -p $pid_ -y
        }
    }
}


function getSzrVersion {
param (
    [string]$path = $installDir
)
    return $(get-content "$path\src\scalarizr\version").trim()
}


function createSzrBackup {
    if (test-path $installDir) {
        log "Backuping current installation $(getSzrVersion)"
        $dirsToBackup | foreach {
            for ($cnt = 0; $cnt -lt 5; $cnt++) {
                log "Renaming $installDir\$_ -> $_-$installedVersion"
                try {
                    rename-item -path "$installDir\$_" -newName "$_-$installedVersion"
                    break
                }
                catch {
                    releaseHandles $installDir
                }
                sleep 1
            }
        }
        echo $null > $backupCreatedLock
    }
}


function restoreSzrBackup {
    if (test-path $backupCreatedLock) {
        log "Restoring previous installation from backup"
        $dirsToBackup | foreach {
            $path = "$installDir\$_-$installedVersion"
            $newName = "$installDir\$_"
            if (test-path $newName) {
                remove-item $newName -force -recurse
            }
            if (test-path $path) {
                rename-item -path $path -newName $newName
            }
        }
        remove-item $backupCreatedLock

        set-itemproperty $uninstallRegKey "DisplayVersion" $installedVersion
        set-itemproperty $uninstallRegKey "DisplayName" "Scalarizr $installedVersion-1"
    }
}


function deleteSzrBackup {
    if (test-path $backupCreatedLock) {
        log "Cleanuping"
        $dirsToBackup | foreach {
            $path = "$installDir\$_-$installedVersion"
            if (test-path $path) {
                log "Remove $path"
                try {
                    remove-item $path -force -recurse
                }
                catch {
                    releaseHandles $path
                }
            }
        }
        remove-item $backupCreatedLock
    }
}


function stopSzrService {
param (
    $name
)
    log "Stopping $Name"
    $job = start-job -scriptBlock {
        $debugPreference = "continue"
        $errorActionPreference = "stop"
        stop-service $using:name
    }
    wait-job $job -timeout 30
    if ($job.state -eq "Running") {
        log "Killing job $($job.Id)"
        stop-job $job
    } else {
        receive-job $job
    }
    $svs = get-wmiobject -class win32_service -filter "name = '$name'"
    if ($svs -and $svs.processId) {
        stop-process -id $svs.processId -force -errorAction stop
    }
}

function stopAllSzrServices {
    log "Stopping services"
    $servicesToOperate | foreach {
        stopSzrService $_
    }
    sleep 2  # Give them time to shutdown
}


function startAllSzrServices {
param (
    [switch] $force
)
    if ($force -or (get-service "ScalrUpdClient" -errorAction silentlyContinue)) {
        #log "Starting services"
        @("ScalrUpdClient") | foreach {
        #$servicesToOperate | foreach {
            $name = $_
            log "Starting $name"
            start-service $name
            log "Service $name started, waiting"  
            sleep 2
            $svs = get-service $name
            log "$name status: $($svs.status)"
            if (-not ($svs.status -eq "Running")) {
                throw "Service $name failed a moment after startup"
            }
        }
    }
}


function setSzrState {
param (
    $state
)
    $script:prevState = $script:state
    $script:state = $state
}


function saveSzrStatus {
    $msg = @()
    $error | foreach { $msg += [string]$_ }
    $msg = $msg | select -uniq
    if ($msg) {  
        # Empty arrays are not welcome: 
        # Exception calling "Reverse" with "1" argument(s): "Value cannot be null.
        [array]::reverse($msg)
    }
    $version = $(get-itemproperty $uninstallRegKey "DisplayVersion").DisplayVersion
    $release = $(get-itemproperty $uninstallRegKey "DisplayRelease").DisplayRelease
    $installed = "$version-$release"

    $status = @{
        error = $msg -join "`n"; 
        state = $state;
        prev_state = $prevState;
        installed = $installed
    } | convertto-json
    log "Saving status: $Status"
    set-content -encoding ascii -path $statusFile -value $status  
}


function main {
    try {
        setSzrState "in-progress/prepare"
        if (test-path $statusFile) {
            remove-item $statusFile
        }
        if (test-path $installDir) {
            $script:installedVersion = getSzrVersion
        }
        try {
            setSzrState "in-progress/download-package"
            $packageFile = downloadFile $url
            if (-not $packageFile) {
                stopAllSzrServices
                throw "Download installer failed"
            }
            setSzrState "in-progress/stop"
            stopAllSzrServices
            createSzrBackup
            try {
                setSzrState "in-progress/install"
                runInstaller $packageFile
                sleep 1  # cause sometimes we've got false positives
                if (-not (test-path "$installDir\src")) {
                    dir $installDir
                    throw "Installer completed without installing new files"
                }
                setSzrState "in-progress/restart"
                saveSzrStatus
                startAllSzrServices -force
                setSzrState "completed"
            }
            catch {
                write-error $_ -errorAction continue
                stopAllSzrServices
                restoreSzrBackup
                setSzrState "rollbacked"
            }
            finally {
                deleteSzrBackup
            }
        }
        catch {
            write-error $_ -errorAction continue
            setSzrState "error"
        }
    }
    finally {
        try {
            saveSzrStatus
            startAllSzrServices -errorAction continue
            remove-Item $packageFile  
        } 
        catch {
            write-error $_ -errorAction continue
        }     
    }
}

main -errorAction continue 5>&1 2>&1 | tee-object -append $logFile

