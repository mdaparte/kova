; Windows installer for the Kova release.
; Built on a Windows runner by packaging/windows/build-setup.ps1.
; The image is not signed.

#ifndef BinDir
  #error Pass /DBinDir= with kova.exe, kova-setup.exe, kovadump.exe, kova.dll, and iggy-server.exe
#endif

#define AppVersion "0.1.1"

[Setup]
AppId={{7C4E9A2B-1D58-4F63-8E0A-6B2F4C8D9E10}
AppName=Kova
AppVersion={#AppVersion}
AppVerName=Kova {#AppVersion}
AppPublisher=Marcelo Daparte
AppPublisherURL=https://github.com/mdaparte/kova
AppSupportURL=https://github.com/mdaparte/kova
DefaultDirName={autopf}\Kova
DefaultGroupName=Kova
DisableProgramGroupPage=yes
OutputBaseFilename=Kova-x64-setup
SetupIconFile=kova.ico
UninstallDisplayIcon={app}\kova.ico
Compression=lzma2
SolidCompression=yes
WizardStyle=modern
ArchitecturesAllowed=x64
ArchitecturesInstallIn64BitMode=x64
PrivilegesRequired=admin
MinVersion=10.0
LicenseFile=..\..\LICENSE
ChangesEnvironment=yes

[Tasks]
Name: desktopicon; Description: "Add a desktop shortcut"; GroupDescription: "Shortcuts:"; Flags: unchecked

[Files]
Source: "{#BinDir}\kova.exe"; DestDir: "{app}"; Flags: ignoreversion
Source: "{#BinDir}\kova-setup.exe"; DestDir: "{app}"; Flags: ignoreversion
Source: "{#BinDir}\kovadump.exe"; DestDir: "{app}"; Flags: ignoreversion
Source: "{#BinDir}\kova.dll"; DestDir: "{app}"; Flags: ignoreversion
Source: "{#BinDir}\iggy-server.exe"; DestDir: "{app}"; Flags: ignoreversion
Source: "{#BinDir}\LICENSE-iggy"; DestDir: "{app}"; Flags: ignoreversion skipifsourcedoesntexist
Source: "{#BinDir}\NOTICE-iggy"; DestDir: "{app}"; Flags: ignoreversion skipifsourcedoesntexist
Source: "{#BinDir}\DEPENDENCIES-iggy"; DestDir: "{app}"; Flags: ignoreversion skipifsourcedoesntexist
Source: "..\kova-setup-window.py"; DestDir: "{app}\packaging"; Flags: ignoreversion
Source: "..\iggy-server-config.py"; DestDir: "{app}\packaging"; Flags: ignoreversion
Source: "..\..\LICENSE"; DestDir: "{app}"; Flags: ignoreversion
Source: "..\..\NOTICE"; DestDir: "{app}"; Flags: ignoreversion
Source: "kova.ico"; DestDir: "{app}"; Flags: ignoreversion

; 64-bit view. uninsdeletevalue on the shared lists, uninsdeletekey only on the Kova keys.
[Registry]
Root: HKLM; Subkey: "SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers"; ValueType: string; ValueName: "Kova"; ValueData: "Installed"; Flags: uninsdeletevalue
Root: HKLM; Subkey: "SOFTWARE\ODBC\ODBCINST.INI\Kova"; ValueType: string; ValueName: "Driver"; ValueData: "{app}\kova.dll"; Flags: uninsdeletekey
Root: HKLM; Subkey: "SOFTWARE\ODBC\ODBCINST.INI\Kova"; ValueType: string; ValueName: "Setup"; ValueData: "{app}\kova.dll"
Root: HKLM; Subkey: "SOFTWARE\ODBC\ODBCINST.INI\Kova"; ValueType: dword; ValueName: "UsageCount"; ValueData: "1"
Root: HKLM; Subkey: "SOFTWARE\ODBC\ODBC.INI\ODBC Data Sources"; ValueType: string; ValueName: "Kova"; ValueData: "Kova"; Flags: uninsdeletevalue
Root: HKLM; Subkey: "SOFTWARE\ODBC\ODBC.INI\Kova"; ValueType: string; ValueName: "Driver"; ValueData: "{app}\kova.dll"; Flags: uninsdeletekey
Root: HKLM; Subkey: "SOFTWARE\ODBC\ODBC.INI\Kova"; ValueType: string; ValueName: "SERVER"; ValueData: "127.0.0.1"
Root: HKLM; Subkey: "SOFTWARE\ODBC\ODBC.INI\Kova"; ValueType: string; ValueName: "PORT"; ValueData: "27018"
Root: HKLM; Subkey: "SOFTWARE\ODBC\ODBC.INI\Kova"; ValueType: string; ValueName: "DATABASE"; ValueData: "db"
Root: HKLM; Subkey: "SOFTWARE\ODBC\ODBC.INI\Kova"; ValueType: string; ValueName: "Description"; ValueData: "Kova"

[Icons]
Name: "{autoprograms}\Kova"; Filename: "{app}\kova-setup.exe"; Parameters: "--window"; IconFilename: "{app}\kova.ico"
Name: "{autodesktop}\Kova"; Filename: "{app}\kova-setup.exe"; Parameters: "--window"; IconFilename: "{app}\kova.ico"; Tasks: desktopicon

[Run]
Filename: "{app}\kova-setup.exe"; Parameters: "--window"; Description: "Configure Kova"; Flags: postinstall nowait skipifsilent

[Code]
const
  EnvironmentKey = 'SYSTEM\CurrentControlSet\Control\Session Manager\Environment';

function EnvContains(const Paths, Item: string): Boolean;
begin
  Result := Pos(';' + Uppercase(Item) + ';', ';' + Uppercase(Paths) + ';') > 0;
end;

procedure EnvAdd(const Item: string);
var
  Paths: string;
begin
  if not RegQueryStringValue(HKLM, EnvironmentKey, 'Path', Paths) then
    Paths := '';
  if EnvContains(Paths, Item) then
    exit;
  if Paths = '' then
    Paths := Item
  else
    Paths := Paths + ';' + Item;
  RegWriteExpandStringValue(HKLM, EnvironmentKey, 'Path', Paths);
end;

procedure EnvRemove(const Item: string);
var
  Paths, Part, Rest, Built: string;
  SplitAt: Integer;
begin
  if not RegQueryStringValue(HKLM, EnvironmentKey, 'Path', Paths) then
    exit;
  Built := '';
  Rest := Paths;
  while Rest <> '' do
  begin
    SplitAt := Pos(';', Rest);
    if SplitAt = 0 then
    begin
      Part := Rest;
      Rest := '';
    end
    else
    begin
      Part := Copy(Rest, 1, SplitAt - 1);
      Rest := Copy(Rest, SplitAt + 1, MaxInt);
    end;
    if (Part <> '') and (CompareText(Part, Item) <> 0) then
    begin
      if Built <> '' then
        Built := Built + ';';
      Built := Built + Part;
    end;
  end;
  RegWriteExpandStringValue(HKLM, EnvironmentKey, 'Path', Built);
end;

procedure StopService(const Name: string);
var
  ResultCode: Integer;
begin
  Exec(ExpandConstant('{sys}\sc.exe'), 'stop ' + Name, '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
  Exec(ExpandConstant('{sys}\sc.exe'), 'delete ' + Name, '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
end;

procedure CurStepChanged(CurStep: TSetupStep);
begin
  if CurStep = ssPostInstall then
  begin
    EnvAdd(ExpandConstant('{app}'));
  end;
end;

procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
begin
  if CurUninstallStep = usUninstall then
  begin
    StopService('Kova');
    StopService('KovaIggy');
  end;
  if CurUninstallStep = usPostUninstall then
  begin
    EnvRemove(ExpandConstant('{app}'));
  end;
end;
