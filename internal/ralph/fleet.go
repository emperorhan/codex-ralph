package ralph

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const fleetConfigVersion = 1

type FleetProject struct {
	ID            string   `json:"id"`
	ProjectDir    string   `json:"project_dir"`
	Plugin        string   `json:"plugin"`
	PRDPath       string   `json:"prd_path,omitempty"`
	AssignedRoles []string `json:"assigned_roles"`
	CreatedAtUTC  string   `json:"created_at_utc"`
}

type FleetConfig struct {
	Version  int            `json:"version"`
	Projects []FleetProject `json:"projects"`
}

func fleetDir(controlDir string) string {
	return filepath.Join(controlDir, "fleet")
}

func fleetConfigPath(controlDir string) string {
	return filepath.Join(fleetDir(controlDir), "projects.json")
}

func LoadFleetConfig(controlDir string) (FleetConfig, error) {
	path := fleetConfigPath(controlDir)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return FleetConfig{Version: fleetConfigVersion, Projects: []FleetProject{}}, nil
		}
		return FleetConfig{}, fmt.Errorf("read fleet config: %w", err)
	}

	cfg := FleetConfig{}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return FleetConfig{}, fmt.Errorf("parse fleet config: %w", err)
	}
	if cfg.Version == 0 {
		cfg.Version = fleetConfigVersion
	}
	if cfg.Projects == nil {
		cfg.Projects = []FleetProject{}
	}
	for i := range cfg.Projects {
		cfg.Projects[i].AssignedRoles = NormalizeRequiredRoles(cfg.Projects[i].AssignedRoles)
		if err := ValidateRequiredRoleSet(cfg.Projects[i].AssignedRoles); err != nil {
			return FleetConfig{}, fmt.Errorf("invalid role set for project %s: %w", cfg.Projects[i].ID, err)
		}
	}
	return cfg, nil
}

func SaveFleetConfig(controlDir string, cfg FleetConfig) error {
	cfg.Version = fleetConfigVersion
	if cfg.Projects == nil {
		cfg.Projects = []FleetProject{}
	}
	if err := os.MkdirAll(fleetDir(controlDir), 0o755); err != nil {
		return fmt.Errorf("create fleet dir: %w", err)
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal fleet config: %w", err)
	}
	if err := os.WriteFile(fleetConfigPath(controlDir), append(data, '\n'), 0o644); err != nil {
		return fmt.Errorf("write fleet config: %w", err)
	}
	return nil
}

func RegisterFleetProject(controlDir, id, projectDir, plugin, prdPath string) (FleetProject, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return FleetProject{}, fmt.Errorf("project id is required")
	}
	for _, ch := range id {
		if !(ch == '-' || ch == '_' || ch == '.' || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9')) {
			return FleetProject{}, fmt.Errorf("project id contains unsupported character: %q", ch)
		}
	}

	if strings.TrimSpace(projectDir) == "" {
		return FleetProject{}, fmt.Errorf("project-dir is required")
	}
	absProject, err := filepath.Abs(projectDir)
	if err != nil {
		return FleetProject{}, fmt.Errorf("resolve project-dir: %w", err)
	}
	if err := os.MkdirAll(absProject, 0o755); err != nil {
		return FleetProject{}, fmt.Errorf("create project-dir: %w", err)
	}

	if plugin == "" {
		plugin = "universal-default"
	}
	if _, err := os.Stat(pluginFilePath(controlDir, plugin)); err != nil {
		return FleetProject{}, fmt.Errorf("plugin not found: %s", plugin)
	}

	cfg, err := LoadFleetConfig(controlDir)
	if err != nil {
		return FleetProject{}, err
	}
	for _, p := range cfg.Projects {
		if p.ID == id {
			return FleetProject{}, fmt.Errorf("fleet project already exists: %s", id)
		}
		if samePath(p.ProjectDir, absProject) {
			return FleetProject{}, fmt.Errorf("project-dir already registered by %s: %s", p.ID, absProject)
		}
	}

	fp := FleetProject{
		ID:            id,
		ProjectDir:    absProject,
		Plugin:        plugin,
		PRDPath:       strings.TrimSpace(prdPath),
		AssignedRoles: append([]string(nil), RequiredAgentRoles...),
		CreatedAtUTC:  time.Now().UTC().Format(time.RFC3339),
	}

	cfg.Projects = append(cfg.Projects, fp)
	if err := SaveFleetConfig(controlDir, cfg); err != nil {
		return FleetProject{}, err
	}
	return fp, nil
}

func UnregisterFleetProject(controlDir, id string) error {
	cfg, err := LoadFleetConfig(controlDir)
	if err != nil {
		return err
	}

	idx := -1
	for i, p := range cfg.Projects {
		if p.ID == id {
			idx = i
			break
		}
	}
	if idx < 0 {
		return fmt.Errorf("fleet project not found: %s", id)
	}

	cfg.Projects = append(cfg.Projects[:idx], cfg.Projects[idx+1:]...)
	return SaveFleetConfig(controlDir, cfg)
}

func FindFleetProject(cfg FleetConfig, id string) (FleetProject, bool) {
	for _, p := range cfg.Projects {
		if p.ID == id {
			return p, true
		}
	}
	return FleetProject{}, false
}

func ResolveFleetProjects(controlDir, projectID string, all bool) ([]FleetProject, error) {
	cfg, err := LoadFleetConfig(controlDir)
	if err != nil {
		return nil, err
	}
	if len(cfg.Projects) == 0 {
		return nil, fmt.Errorf("fleet is empty. register project first")
	}

	if all {
		return cfg.Projects, nil
	}
	if strings.TrimSpace(projectID) == "" {
		return nil, fmt.Errorf("either --id or --all is required")
	}
	project, ok := FindFleetProject(cfg, projectID)
	if !ok {
		return nil, fmt.Errorf("fleet project not found: %s", projectID)
	}
	return []FleetProject{project}, nil
}

func EnsureFleetProjectInstalled(paths Paths, plugin, executablePath string) error {
	if err := EnsureLayout(paths); err != nil {
		return err
	}
	wrapperPath := filepath.Join(paths.ProjectDir, "ralph")
	if _, err := os.Stat(wrapperPath); os.IsNotExist(err) {
		return Install(paths, plugin, executablePath)
	} else if err != nil {
		return fmt.Errorf("check wrapper: %w", err)
	}

	_, yamlErr := os.Stat(paths.ProfileYAMLFile)
	_, envErr := os.Stat(paths.ProfileFile)
	if os.IsNotExist(yamlErr) && os.IsNotExist(envErr) {
		return ApplyPlugin(paths, plugin)
	}
	if yamlErr != nil && !os.IsNotExist(yamlErr) {
		return fmt.Errorf("check profile.yaml: %w", yamlErr)
	}
	if envErr != nil && !os.IsNotExist(envErr) {
		return fmt.Errorf("check profile.env: %w", envErr)
	}
	return nil
}

func EnsureFleetAgentSetFile(paths Paths, project FleetProject) error {
	if err := ValidateRequiredRoleSet(project.AssignedRoles); err != nil {
		return err
	}
	if err := EnsureLayout(paths); err != nil {
		return err
	}
	prd := project.PRDPath
	if prd == "" {
		prd = "PRD.md"
	}
	content := fmt.Sprintf(
		"PROJECT_ID=%s\nPROJECT_DIR=%s\nPRD_PATH=%s\nAGENT_SET_ROLES=%s\nAGENT_SET_SIZE=4\nUPDATED_AT_UTC=%s\n",
		project.ID,
		project.ProjectDir,
		prd,
		strings.Join(project.AssignedRoles, ","),
		time.Now().UTC().Format(time.RFC3339),
	)
	return os.WriteFile(paths.AgentSetFile, []byte(content), 0o644)
}

func samePath(a, b string) bool {
	ca := filepath.Clean(a)
	cb := filepath.Clean(b)
	return ca == cb
}
