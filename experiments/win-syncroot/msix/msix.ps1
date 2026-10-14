# Does a packaged app need the cloud files extension to register a sync root, and what can it do
# with the root's own status? Runs a few sequences unpackaged (the baseline that worked on a real
# PC) and packaged with the extension (as Filesync 0.5.3), and shows what Windows made of them.
# (Packaged without the extension, as 0.5.2: listing sync roots fails and unregistering crashes;
# shown in this workflow's earlier runs.) For a CI runner: it turns on developer mode to register
# a loose package.
param([Parameter(Mandatory)] [string] $Exe)

$ErrorActionPreference = 'Continue'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$logo = Join-Path $here '..\..\..\crates\hbsync-tray\assets\mark-256.png'
$extension = '<desktop3:Extension Category="windows.cloudFiles"><desktop3:CloudFiles /></desktop3:Extension>'

reg add 'HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\AppModelUnlock' /t REG_DWORD /f /v AllowDevelopmentWithoutDevLicense /d 1 | Out-Null
[Environment]::OSVersion.Version

# Each inner list is one process's commands, in order.
$sequences = @(
    @('register'),
    @('in-sync root-in-sync-win32 pin unpin pin status unregister-bare')
)

function Try-It([string] $title, [string] $run) {
    foreach ($steps in $sequences) {
        Write-Output ''
        Write-Output "===== $title`: $($steps -join ', ')"
        foreach ($step in $steps) {
            $out = & $run ($step -split ' ') 2>&1 | Where-Object { $_ -notmatch 'Explorer|sign out|NEW' }
            Write-Output "$step -> exit $LASTEXITCODE  $($out -join ' / ')"
        }
    }
    Write-Output '--- navigation pane (HKCU Desktop\NameSpace) afterwards'
    Get-ChildItem 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Explorer\Desktop\NameSpace' -ErrorAction SilentlyContinue |
        ForEach-Object { '{0}  {1}' -f $_.PSChildName, $_.GetValue('') }
}

Try-It 'unpackaged' $Exe

$name = 'HomebaseSyncrootCloudFiles'
$alias = 'hb-syncroot-cloudfiles.exe'
$layout = Join-Path $env:RUNNER_TEMP $name
New-Item -ItemType Directory -Force $layout | Out-Null
Copy-Item $Exe $layout
Copy-Item $logo (Join-Path $layout 'logo.png')
(Get-Content -Raw (Join-Path $here 'AppxManifest.xml')).
    Replace('{NAME}', $name).Replace('{ALIAS}', $alias).Replace('{EXT}', $extension) |
    Set-Content -Encoding utf8 (Join-Path $layout 'AppxManifest.xml')
Add-AppxPackage -Register (Join-Path $layout 'AppxManifest.xml')
Start-Sleep 2
Try-It 'packaged' (Join-Path $env:LOCALAPPDATA "Microsoft\WindowsApps\$alias")
exit 0
