package ralph

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

type ServicePlatform string

const (
	ServicePlatformSystemd ServicePlatform = "systemd-user"
	ServicePlatformLaunchd ServicePlatform = "launchd"
)

type ServiceInstallResult struct {
	Platform    ServicePlatform
	ServiceName string
	UnitPath    string
	Activated   bool
	Warnings    []string
}

type ServiceStatus struct {
	Platform    ServicePlatform
	ServiceName string
	Installed   bool
	Active      bool
	Detail      string
}

func DetectServicePlatform() (ServicePlatform, error) {
	switch runtime.GOOS {
	case "linux":
		return ServicePlatformSystemd, nil
	case "darwin":
		return ServicePlatformLaunchd, nil
	default:
		return "", fmt.Errorf("unsupported platform: %s", runtime.GOOS)
	}
}

func DefaultServiceName(projectDir string) string {
	base := filepath.Base(strings.TrimSpace(projectDir))
	if base == "" || base == "." || base == string(filepath.Separator) {
		base = "project"
	}
	return "ralph-" + sanitizeServiceToken(base)
}

func InstallService(paths Paths, executablePath, serviceName string, activate bool) (ServiceInstallResult, error) {
	if err := EnsureLayout(paths); err != nil {
		return ServiceInstallResult{}, err
	}

	platform, err := DetectServicePlatform()
	if err != nil {
		return ServiceInstallResult{}, err
	}
	serviceName = normalizeServiceName(serviceName, paths.ProjectDir)

	switch platform {
	case ServicePlatformSystemd:
		return installSystemdUserService(paths, executablePath, serviceName, activate)
	case ServicePlatformLaunchd:
		return installLaunchdService(paths, executablePath, serviceName, activate)
	default:
		return ServiceInstallResult{}, fmt.Errorf("unsupported service platform: %s", platform)
	}
}

func UninstallService(paths Paths, serviceName string) (ServiceInstallResult, error) {
	platform, err := DetectServicePlatform()
	if err != nil {
		return ServiceInstallResult{}, err
	}
	serviceName = normalizeServiceName(serviceName, paths.ProjectDir)

	switch platform {
	case ServicePlatformSystemd:
		return uninstallSystemdUserService(paths, serviceName)
	case ServicePlatformLaunchd:
		return uninstallLaunchdService(paths, serviceName)
	default:
		return ServiceInstallResult{}, fmt.Errorf("unsupported service platform: %s", platform)
	}
}

func GetServiceStatus(paths Paths, serviceName string) (ServiceStatus, error) {
	platform, err := DetectServicePlatform()
	if err != nil {
		return ServiceStatus{}, err
	}
	serviceName = normalizeServiceName(serviceName, paths.ProjectDir)

	switch platform {
	case ServicePlatformSystemd:
		return getSystemdUserServiceStatus(serviceName)
	case ServicePlatformLaunchd:
		return getLaunchdServiceStatus(serviceName)
	default:
		return ServiceStatus{}, fmt.Errorf("unsupported service platform: %s", platform)
	}
}

func normalizeServiceName(serviceName, projectDir string) string {
	name := sanitizeServiceToken(strings.TrimSpace(serviceName))
	if name == "" {
		return DefaultServiceName(projectDir)
	}
	if strings.HasSuffix(name, ".service") {
		name = strings.TrimSuffix(name, ".service")
	}
	return name
}

func sanitizeServiceToken(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range raw {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r + ('a' - 'A'))
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_' || r == '.':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	out := strings.Trim(b.String(), "-_.")
	return out
}

func installSystemdUserService(paths Paths, executablePath, serviceName string, activate bool) (ServiceInstallResult, error) {
	unitPath, err := DefaultLinuxServicePath(serviceName)
	if err != nil {
		return ServiceInstallResult{}, err
	}
	if err := os.MkdirAll(filepath.Dir(unitPath), 0o755); err != nil {
		return ServiceInstallResult{}, fmt.Errorf("create systemd user dir: %w", err)
	}

	unitContent := fmt.Sprintf(`[Unit]
Description=Ralph Autonomous Loop (%s)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=%s
ExecStart=%s --control-dir %s --project-dir %s supervise
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
`,
		serviceName,
		systemdEscape(paths.ProjectDir),
		systemdEscape(executablePath),
		systemdEscape(paths.ControlDir),
		systemdEscape(paths.ProjectDir),
	)
	if err := os.WriteFile(unitPath, []byte(unitContent), 0o644); err != nil {
		return ServiceInstallResult{}, fmt.Errorf("write systemd unit: %w", err)
	}

	result := ServiceInstallResult{
		Platform:    ServicePlatformSystemd,
		ServiceName: serviceName,
		UnitPath:    unitPath,
	}
	if !activate {
		return result, nil
	}

	if err := runCommand("systemctl", "--user", "daemon-reload"); err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("daemon-reload failed: %v", err))
		return result, nil
	}
	if err := runCommand("systemctl", "--user", "enable", "--now", serviceName+".service"); err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("enable --now failed: %v", err))
		return result, nil
	}
	result.Activated = true
	return result, nil
}

func uninstallSystemdUserService(paths Paths, serviceName string) (ServiceInstallResult, error) {
	_ = paths
	unitPath, err := DefaultLinuxServicePath(serviceName)
	if err != nil {
		return ServiceInstallResult{}, err
	}
	result := ServiceInstallResult{
		Platform:    ServicePlatformSystemd,
		ServiceName: serviceName,
		UnitPath:    unitPath,
	}
	if err := runCommand("systemctl", "--user", "disable", "--now", serviceName+".service"); err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("disable --now failed: %v", err))
	}
	if err := os.Remove(unitPath); err != nil && !os.IsNotExist(err) {
		return result, fmt.Errorf("remove systemd unit: %w", err)
	}
	if err := runCommand("systemctl", "--user", "daemon-reload"); err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("daemon-reload failed: %v", err))
	}
	return result, nil
}

func getSystemdUserServiceStatus(serviceName string) (ServiceStatus, error) {
	unitPath, err := DefaultLinuxServicePath(serviceName)
	if err != nil {
		return ServiceStatus{}, err
	}
	st := ServiceStatus{
		Platform:    ServicePlatformSystemd,
		ServiceName: serviceName,
	}
	if _, err := os.Stat(unitPath); err != nil {
		if os.IsNotExist(err) {
			st.Detail = "service file not installed"
			return st, nil
		}
		return st, err
	}
	st.Installed = true
	if err := runCommand("systemctl", "--user", "is-active", "--quiet", serviceName+".service"); err == nil {
		st.Active = true
		st.Detail = "active"
		return st, nil
	}
	st.Detail = "inactive"
	return st, nil
}

func installLaunchdService(paths Paths, executablePath, serviceName string, activate bool) (ServiceInstallResult, error) {
	label := launchdLabel(serviceName)
	plistPath, err := DefaultDarwinServicePath(label)
	if err != nil {
		return ServiceInstallResult{}, err
	}
	if err := os.MkdirAll(filepath.Dir(plistPath), 0o755); err != nil {
		return ServiceInstallResult{}, fmt.Errorf("create launchagents dir: %w", err)
	}

	stdoutPath := filepath.Join(paths.LogsDir, "launchd.out.log")
	stderrPath := filepath.Join(paths.LogsDir, "launchd.err.log")
	content := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>%s</string>
  <key>ProgramArguments</key>
  <array>
    <string>%s</string>
    <string>--control-dir</string>
    <string>%s</string>
    <string>--project-dir</string>
    <string>%s</string>
    <string>supervise</string>
  </array>
  <key>WorkingDirectory</key>
  <string>%s</string>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <dict>
    <key>SuccessfulExit</key>
    <false/>
  </dict>
  <key>StandardOutPath</key>
  <string>%s</string>
  <key>StandardErrorPath</key>
  <string>%s</string>
</dict>
</plist>
`,
		xmlEscape(label),
		xmlEscape(executablePath),
		xmlEscape(paths.ControlDir),
		xmlEscape(paths.ProjectDir),
		xmlEscape(paths.ProjectDir),
		xmlEscape(stdoutPath),
		xmlEscape(stderrPath),
	)
	if err := os.WriteFile(plistPath, []byte(content), 0o644); err != nil {
		return ServiceInstallResult{}, fmt.Errorf("write launchd plist: %w", err)
	}

	result := ServiceInstallResult{
		Platform:    ServicePlatformLaunchd,
		ServiceName: label,
		UnitPath:    plistPath,
	}
	if !activate {
		return result, nil
	}

	_ = runCommand("launchctl", "unload", plistPath)
	if err := runCommand("launchctl", "load", "-w", plistPath); err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("launchctl load failed: %v", err))
		return result, nil
	}
	result.Activated = true
	return result, nil
}

func uninstallLaunchdService(paths Paths, serviceName string) (ServiceInstallResult, error) {
	_ = paths
	label := launchdLabel(serviceName)
	plistPath, err := DefaultDarwinServicePath(label)
	if err != nil {
		return ServiceInstallResult{}, err
	}
	result := ServiceInstallResult{
		Platform:    ServicePlatformLaunchd,
		ServiceName: label,
		UnitPath:    plistPath,
	}
	if err := runCommand("launchctl", "unload", "-w", plistPath); err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("launchctl unload failed: %v", err))
	}
	if err := os.Remove(plistPath); err != nil && !os.IsNotExist(err) {
		return result, fmt.Errorf("remove launchd plist: %w", err)
	}
	return result, nil
}

func getLaunchdServiceStatus(serviceName string) (ServiceStatus, error) {
	label := launchdLabel(serviceName)
	plistPath, err := DefaultDarwinServicePath(label)
	if err != nil {
		return ServiceStatus{}, err
	}

	st := ServiceStatus{
		Platform:    ServicePlatformLaunchd,
		ServiceName: label,
	}
	if _, err := os.Stat(plistPath); err != nil {
		if os.IsNotExist(err) {
			st.Detail = "service file not installed"
			return st, nil
		}
		return st, err
	}
	st.Installed = true
	if err := runCommand("launchctl", "list", label); err == nil {
		st.Active = true
		st.Detail = "loaded"
		return st, nil
	}
	st.Detail = "not loaded"
	return st, nil
}

func runCommand(name string, args ...string) error {
	if _, err := exec.LookPath(name); err != nil {
		return fmt.Errorf("%s not found", name)
	}
	cmd := exec.Command(name, args...)
	output, err := cmd.CombinedOutput()
	if err == nil {
		return nil
	}
	text := strings.TrimSpace(string(output))
	if text == "" {
		return err
	}
	return fmt.Errorf("%v: %s", err, text)
}

func systemdEscape(value string) string {
	if value == "" {
		return "\"\""
	}
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range value {
		switch r {
		case '\\', '"':
			b.WriteByte('\\')
			b.WriteRune(r)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}

func xmlEscape(value string) string {
	replacer := strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		`"`, "&quot;",
		"'", "&apos;",
	)
	return replacer.Replace(value)
}

func ServiceInstallHint(platform ServicePlatform) string {
	switch platform {
	case ServicePlatformSystemd:
		return "manage with: systemctl --user status|restart <service>.service"
	case ServicePlatformLaunchd:
		uid := strconv.Itoa(os.Getuid())
		return "manage with: launchctl list|kickstart gui/" + uid + "/<label>"
	default:
		return ""
	}
}

func launchdLabel(serviceName string) string {
	name := strings.TrimSpace(serviceName)
	if strings.HasPrefix(name, "io.ralph.") {
		suffix := strings.TrimPrefix(name, "io.ralph.")
		return "io.ralph." + sanitizeServiceToken(suffix)
	}
	return "io.ralph." + sanitizeServiceToken(name)
}
