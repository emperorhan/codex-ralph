package ralph

import (
	"fmt"
	"strings"
)

var RequiredAgentRoles = []string{"manager", "planner", "developer", "qa"}

func IsSupportedRole(role string) bool {
	switch strings.TrimSpace(role) {
	case "manager", "planner", "developer", "qa":
		return true
	default:
		return false
	}
}

func ParseRolesCSV(raw string) (map[string]struct{}, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil
	}
	out := map[string]struct{}{}
	for _, part := range strings.Split(trimmed, ",") {
		role := strings.TrimSpace(part)
		if role == "" {
			continue
		}
		if !IsSupportedRole(role) {
			return nil, fmt.Errorf("unsupported role: %s", role)
		}
		out[role] = struct{}{}
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func RoleSetCSV(roles map[string]struct{}) string {
	if len(roles) == 0 {
		return ""
	}
	items := make([]string, 0, len(roles))
	for _, role := range RequiredAgentRoles {
		if _, ok := roles[role]; ok {
			items = append(items, role)
		}
	}
	return strings.Join(items, ",")
}

func RequiredRoleSet() map[string]struct{} {
	out := map[string]struct{}{}
	for _, role := range RequiredAgentRoles {
		out[role] = struct{}{}
	}
	return out
}

func ValidateRequiredRoleSet(roles []string) error {
	set := map[string]struct{}{}
	for _, role := range roles {
		n := strings.TrimSpace(role)
		if n == "" {
			continue
		}
		if !IsSupportedRole(n) {
			return fmt.Errorf("unsupported role in role set: %s", n)
		}
		set[n] = struct{}{}
	}
	for _, role := range RequiredAgentRoles {
		if _, ok := set[role]; !ok {
			return fmt.Errorf("role set must include %s", role)
		}
	}
	if len(set) != len(RequiredAgentRoles) {
		return fmt.Errorf("role set must be exactly manager,planner,developer,qa")
	}
	return nil
}

func NormalizeRequiredRoles(roles []string) []string {
	if len(roles) == 0 {
		return append([]string(nil), RequiredAgentRoles...)
	}
	set := map[string]struct{}{}
	for _, role := range roles {
		if IsSupportedRole(role) {
			set[strings.TrimSpace(role)] = struct{}{}
		}
	}
	out := make([]string, 0, len(RequiredAgentRoles))
	for _, role := range RequiredAgentRoles {
		if _, ok := set[role]; ok {
			out = append(out, role)
		}
	}
	if len(out) == 0 {
		return append([]string(nil), RequiredAgentRoles...)
	}
	return out
}
