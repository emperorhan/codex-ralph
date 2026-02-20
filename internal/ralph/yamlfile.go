package ralph

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

type yamlScope struct {
	indent int
	key    string
}

type yamlPending struct {
	fullKey string
	rawKey  string
	indent  int
}

type yamlListState struct {
	key    string
	indent int
	values []string
}

func ReadYAMLFlatMap(path string) (map[string]string, error) {
	out := map[string]string{}
	f, err := os.Open(path)
	if err != nil {
		return out, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	lineNo := 0
	scopes := []yamlScope{}
	var pending *yamlPending
	var list *yamlListState

	for scanner.Scan() {
		lineNo++
		line := scanner.Text()
		if lineNo == 1 {
			line = strings.TrimPrefix(line, "\uFEFF")
		}
		line = strings.TrimRight(line, " \t")
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}

		indent, indentErr := leadingSpaces(line)
		if indentErr != nil {
			return out, fmt.Errorf("parse yaml %s line %d: %w", path, lineNo, indentErr)
		}

	reprocess:
		if list != nil {
			item := strings.TrimSpace(line)
			if indent > list.indent && strings.HasPrefix(item, "- ") {
				item = strings.TrimSpace(strings.TrimPrefix(item, "-"))
				value, valueErr := parseYAMLScalar(item)
				if valueErr != nil {
					return out, fmt.Errorf("parse yaml %s line %d: %w", path, lineNo, valueErr)
				}
				list.values = append(list.values, value)
				continue
			}
			out[list.key] = strings.Join(list.values, ",")
			list = nil
		}

		if pending != nil {
			item := strings.TrimSpace(line)
			if indent > pending.indent {
				if strings.HasPrefix(item, "- ") {
					list = &yamlListState{
						key:    pending.fullKey,
						indent: pending.indent,
					}
					pending = nil
					goto reprocess
				}
				scopes = append(scopes, yamlScope{
					indent: pending.indent,
					key:    pending.rawKey,
				})
				pending = nil
			} else {
				out[pending.fullKey] = ""
				pending = nil
			}
		}

		for len(scopes) > 0 && indent <= scopes[len(scopes)-1].indent {
			scopes = scopes[:len(scopes)-1]
		}

		trimmed = strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "- ") {
			return out, fmt.Errorf("parse yaml %s line %d: list item without a key", path, lineNo)
		}

		i := strings.Index(trimmed, ":")
		if i <= 0 {
			return out, fmt.Errorf("parse yaml %s line %d: expected key: value", path, lineNo)
		}
		rawKey := strings.TrimSpace(trimmed[:i])
		if rawKey == "" {
			return out, fmt.Errorf("parse yaml %s line %d: empty key", path, lineNo)
		}
		rest := strings.TrimSpace(trimmed[i+1:])

		fullKey := rawKey
		if len(scopes) > 0 {
			parts := make([]string, 0, len(scopes)+1)
			for _, scope := range scopes {
				parts = append(parts, scope.key)
			}
			parts = append(parts, rawKey)
			fullKey = strings.Join(parts, ".")
		}

		if rest == "" {
			pending = &yamlPending{
				fullKey: fullKey,
				rawKey:  rawKey,
				indent:  indent,
			}
			continue
		}

		value, valueErr := parseYAMLValue(rest)
		if valueErr != nil {
			return out, fmt.Errorf("parse yaml %s line %d: %w", path, lineNo, valueErr)
		}
		out[fullKey] = value
	}

	if err := scanner.Err(); err != nil {
		return out, fmt.Errorf("scan yaml file %s: %w", path, err)
	}
	if list != nil {
		out[list.key] = strings.Join(list.values, ",")
	}
	if pending != nil {
		out[pending.fullKey] = ""
	}
	return out, nil
}

func WriteYAMLFlatMap(path string, values map[string]string) error {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteString("# Ralph profile config (YAML)\n")
	b.WriteString("# Priority: default < profile.yaml < profile.local.yaml < *.env < process env\n")
	b.WriteString("# ENV key format is also supported here (e.g. RALPH_IDLE_SLEEP_SEC)\n\n")
	for _, key := range keys {
		value := strings.TrimSpace(values[key])
		if isYAMLListKey(key) {
			items := splitCSVValues(value)
			if len(items) == 0 {
				b.WriteString(key)
				b.WriteString(": []\n")
				continue
			}
			b.WriteString(key)
			b.WriteString(":\n")
			for _, item := range items {
				b.WriteString("  - ")
				b.WriteString(yamlScalar(item))
				b.WriteString("\n")
			}
			continue
		}
		b.WriteString(key)
		b.WriteString(": ")
		b.WriteString(yamlScalar(value))
		b.WriteString("\n")
	}

	return os.WriteFile(path, []byte(b.String()), 0o644)
}

func leadingSpaces(line string) (int, error) {
	count := 0
	for _, r := range line {
		if r == ' ' {
			count++
			continue
		}
		if r == '\t' {
			return 0, fmt.Errorf("tabs are not supported for indentation")
		}
		break
	}
	return count, nil
}

func parseYAMLValue(raw string) (string, error) {
	trimmed := strings.TrimSpace(stripYAMLInlineComment(raw))
	if trimmed == "" {
		return "", nil
	}
	if strings.HasPrefix(trimmed, "[") {
		return parseYAMLInlineList(trimmed)
	}
	return parseYAMLScalar(trimmed)
}

func parseYAMLInlineList(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if !strings.HasPrefix(raw, "[") || !strings.HasSuffix(raw, "]") {
		return "", fmt.Errorf("invalid inline list")
	}
	body := strings.TrimSpace(raw[1 : len(raw)-1])
	if body == "" {
		return "", nil
	}

	items, err := splitInlineList(body)
	if err != nil {
		return "", err
	}
	values := make([]string, 0, len(items))
	for _, item := range items {
		value, valueErr := parseYAMLScalar(strings.TrimSpace(item))
		if valueErr != nil {
			return "", valueErr
		}
		values = append(values, value)
	}
	return strings.Join(values, ","), nil
}

func splitInlineList(raw string) ([]string, error) {
	out := []string{}
	current := strings.Builder{}
	inSingle := false
	inDouble := false

	for i := 0; i < len(raw); i++ {
		ch := raw[i]

		if inSingle {
			if ch == '\'' {
				if i+1 < len(raw) && raw[i+1] == '\'' {
					current.WriteByte(ch)
					current.WriteByte(raw[i+1])
					i++
					continue
				}
				inSingle = false
			}
			current.WriteByte(ch)
			continue
		}
		if inDouble {
			if ch == '\\' && i+1 < len(raw) {
				current.WriteByte(ch)
				i++
				current.WriteByte(raw[i])
				continue
			}
			if ch == '"' {
				inDouble = false
			}
			current.WriteByte(ch)
			continue
		}

		switch ch {
		case '\'':
			inSingle = true
			current.WriteByte(ch)
		case '"':
			inDouble = true
			current.WriteByte(ch)
		case ',':
			out = append(out, strings.TrimSpace(current.String()))
			current.Reset()
		default:
			current.WriteByte(ch)
		}
	}

	if inSingle || inDouble {
		return nil, fmt.Errorf("unterminated quoted value")
	}
	out = append(out, strings.TrimSpace(current.String()))
	return out, nil
}

func parseYAMLScalar(raw string) (string, error) {
	trimmed := strings.TrimSpace(stripYAMLInlineComment(raw))
	if trimmed == "" {
		return "", nil
	}
	if len(trimmed) >= 2 && trimmed[0] == '"' && trimmed[len(trimmed)-1] == '"' {
		value, err := strconv.Unquote(trimmed)
		if err != nil {
			return "", fmt.Errorf("invalid double-quoted string")
		}
		return value, nil
	}
	if len(trimmed) >= 2 && trimmed[0] == '\'' && trimmed[len(trimmed)-1] == '\'' {
		value := strings.ReplaceAll(trimmed[1:len(trimmed)-1], "''", "'")
		return value, nil
	}
	return trimmed, nil
}

func stripYAMLInlineComment(raw string) string {
	inSingle := false
	inDouble := false
	prevIsSpace := true
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if inSingle {
			if ch == '\'' {
				if i+1 < len(raw) && raw[i+1] == '\'' {
					i++
					continue
				}
				inSingle = false
			}
			prevIsSpace = ch == ' ' || ch == '\t'
			continue
		}
		if inDouble {
			if ch == '\\' && i+1 < len(raw) {
				i++
				prevIsSpace = false
				continue
			}
			if ch == '"' {
				inDouble = false
			}
			prevIsSpace = ch == ' ' || ch == '\t'
			continue
		}

		if ch == '\'' {
			inSingle = true
			prevIsSpace = false
			continue
		}
		if ch == '"' {
			inDouble = true
			prevIsSpace = false
			continue
		}
		if ch == '#' && prevIsSpace {
			return strings.TrimSpace(raw[:i])
		}
		prevIsSpace = ch == ' ' || ch == '\t'
	}
	return strings.TrimSpace(raw)
}

func isYAMLListKey(key string) bool {
	normalized := normalizeConfigKey(key)
	return normalized == "validate_roles" || normalized == "ralph_validate_roles"
}

func splitCSVValues(raw string) []string {
	out := []string{}
	for _, part := range strings.Split(raw, ",") {
		value := strings.TrimSpace(part)
		if value == "" {
			continue
		}
		out = append(out, value)
	}
	return out
}

func yamlScalar(raw string) string {
	if raw == "" {
		return `""`
	}
	lower := strings.ToLower(raw)
	if lower == "true" || lower == "false" || lower == "null" {
		return lower
	}
	if _, err := strconv.Atoi(raw); err == nil {
		return raw
	}
	if isPlainYAMLValue(raw) {
		return raw
	}
	return strconv.Quote(raw)
}

func isPlainYAMLValue(raw string) bool {
	for _, r := range raw {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			continue
		}
		switch r {
		case '-', '_', '.', '/':
			continue
		default:
			return false
		}
	}
	return true
}
