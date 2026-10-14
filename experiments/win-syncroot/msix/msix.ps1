# Does a packaged app need the cloud files extension to register a sync root? Registers the
# test folder three ways and shows what Windows made of each: unpackaged (the baseline that
# worked on a real PC), packaged without the extension (Filesync 0.5.2: E_BOUNDS), and packaged
# with it. For a CI runner (it turns on developer mode to register loose packages).
param([Parameter(Mandatory)] [string] $Exe)

$ErrorActionPreference = 'Continue'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$logo = Join-Path $here '..\..\..\crates\hbsync-tray\assets\mark-256.png'
$extension = '<desktop3:Extension Category="windows.cloudFiles"><desktop3:CloudFiles /></desktop3:Extension>'

reg add 'HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\AppModelUnlock' /t REG_DWORD /f /v AllowDevelopmentWithoutDevLicense /d 1 | Out-Null
[Environment]::OSVersion.Version

function Show {
    Write-Output '--- SyncRootManager'
    reg query 'HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\Explorer\SyncRootManager' /s
    Write-Output '--- navigation pane (HKCU Desktop\NameSpace)'
    Get-ChildItem 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Explorer\Desktop\NameSpace' -ErrorAction SilentlyContinue |
        ForEach-Object { '{0}  {1}' -f $_.PSChildName, $_.GetValue('') }
}

function Try-It([string] $title, [string] $run) {
    Write-Output ''
    Write-Output "===== $title"
    & $run register 2>&1
    Write-Output "exit $LASTEXITCODE"
    & $run status 2>&1
    Show
    & $run unregister 2>&1
    Write-Output "unregister: exit $LASTEXITCODE"
}

Try-It 'unpackaged' $Exe

foreach ($v in @(@{ Name = 'HomebaseSyncrootPlain'; Alias = 'hb-syncroot-plain.exe'; Ext = '' },
                 @{ Name = 'HomebaseSyncrootCloudFiles'; Alias = 'hb-syncroot-cloudfiles.exe'; Ext = $extension })) {
    $layout = Join-Path $env:RUNNER_TEMP $v.Name
    New-Item -ItemType Directory -Force $layout | Out-Null
    Copy-Item $Exe $layout
    Copy-Item $logo (Join-Path $layout 'logo.png')
    (Get-Content -Raw (Join-Path $here 'AppxManifest.xml')).
        Replace('{NAME}', $v.Name).Replace('{ALIAS}', $v.Alias).Replace('{EXT}', $v.Ext) |
        Set-Content -Encoding utf8 (Join-Path $layout 'AppxManifest.xml')
    Add-AppxPackage -Register (Join-Path $layout 'AppxManifest.xml')
    if (-not $?) { Write-Output "===== $($v.Name): not registered"; continue }
    Start-Sleep 2
    Try-It "packaged: $($v.Name)" (Join-Path $env:LOCALAPPDATA "Microsoft\WindowsApps\$($v.Alias)")
}
