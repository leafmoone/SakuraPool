# Read-only process check. Do not emit command lines (may contain secrets).
$ErrorActionPreference = 'Stop'
$paths = @(
 'D:\SakuraTool\SakuraPool\.venv',
 'D:\SakuraTool\SakuraPool-P4-work',
 'D:\SakuraTool\SakuraPool-Fix3-20260930a'
)
$venvs = @()
if ($env:PROJECT_VENVS) { $venvs = $env:PROJECT_VENVS.Split('|') }
$records = @()
foreach ($process in Get-CimInstance Win32_Process) {
 $matches = @()
 foreach ($path in $paths) {
  if (($process.ExecutablePath -and $process.ExecutablePath.StartsWith($path, [StringComparison]::OrdinalIgnoreCase)) -or
      ($process.CommandLine -and $process.CommandLine.IndexOf($path, [StringComparison]::OrdinalIgnoreCase) -ge 0)) {
   $matches += $path
  }
 }
 $matchedVenvs = @()
 foreach ($venv in $venvs) {
  if (($process.ExecutablePath -and $process.ExecutablePath.StartsWith($venv + '\', [StringComparison]::OrdinalIgnoreCase)) -or
      ($process.CommandLine -and $process.CommandLine.IndexOf($venv + '\', [StringComparison]::OrdinalIgnoreCase) -ge 0) -or
      ($process.CommandLine -and $process.CommandLine.IndexOf($venv.Replace('\', '/') + '/', [StringComparison]::OrdinalIgnoreCase) -ge 0)) {
   $matchedVenvs += $venv
  }
 }
 if ($matches.Count -gt 0 -or $matchedVenvs.Count -gt 0) {
  $records += [PSCustomObject]@{
   pid = $process.ProcessId; name = $process.Name
   executable = $process.ExecutablePath; matched_roots = $matches; matched_venvs = $matchedVenvs
   commandline_not_recorded = $true
  }
 }
}
ConvertTo-Json -InputObject $records -Depth 4 -Compress
