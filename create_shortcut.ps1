$ws = New-Object -ComObject WScript.Shell
$desktop = $ws.SpecialFolders("Desktop")
$sc = $ws.CreateShortcut("$desktop\Email Pipeline.lnk")
$sc.TargetPath = "pythonw.exe"
$sc.Arguments = '"C:\Users\jakew\OneDrive - Montholme Asset Management Ltd\1. Property Business\05. Technology\AI Apps\introductions_tracker\gui.py"'
$sc.WorkingDirectory = "C:\Users\jakew\OneDrive - Montholme Asset Management Ltd\1. Property Business\05. Technology\AI Apps\introductions_tracker"
$sc.Description = "Investment Email Pipeline GUI"
$sc.Save()
Write-Host "Shortcut created on Desktop: Email Pipeline.lnk"
