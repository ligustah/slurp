# Development Guidelines

## Development Process

1. **Create placeholder functions with documentation**
   - Write function signatures with precise, concise doc comments
   - Document the contract before implementing

2. **Write tests first**
   - Create tests that verify the documented behavior
   - Use table-driven tests for multiple scenarios

3. **Implement and verify**
   - Fill in the method body
   - Run tests to verify behavior
   - If tests fail, think _VERY HARD_: is the expectation wrong or the implementation bad?

4. **Keep it simple**
   - Strongly prefer simpler implementations over complex logic
   - Don't over-engineer

## Tool Preferences

- **Use MCP tools**: Prefer `gopls` over grep searches, `delve` over printf debugging
- `mcp__gopls__*` - For code navigation, references, documentation
- `mcp__delve__*` - For debugging, stepping through code

## Anti-Cleverness Guidelines

### 1. Read First, Code Second
- Before implementing, understand what already exists
- When told to reference external code, read it completely first
- Ask: "What does existing implementation actually do?" not "What would be a good way?"

### 2. Explicit Constraints Are Hard Constraints
- Treat "DO NOT use X" or "NO Y" as absolute constraints
- If you catch yourself thinking "but what if I...", stop and re-read constraints

### 3. Resist the "Surely They Mean..." Trap
- Don't interpret requirements beyond what's written
- When in doubt, follow the literal instruction

### 4. Simplicity Bias
- If an implementation looks "too simple", it probably IS simple
- Complex solutions are often wrong when simple approaches exist

### 5. Stop and Check Rule
- Before implementing any algorithm, ask: "Did they explicitly tell me NOT to do this?"

## Go Code Guidelines

### Style
- Use the most idiomatic Go code
- Use meaningful variable and function names
- Write clear, concise comments for complex logic; avoid redundant comments
- Avoid magic numbers/strings; use constants
- Keep functions small and focused on a single task

### Tools
- Always use `go vet` to check code
- Always use `go fmt` to format code when done
- Use `gopls` MCP tools to lookup references/documentation (avoid grep)
- Use `go doc` to review documentation
- Use `go test` to run tests before committing

### Dependencies
- Prefer stdlib packages over third-party libraries unless necessary
- Use existing packages in the codebase where applicable
- Use `go mod` for dependency management; keep dependencies up to date

### Error Handling
- Always check for errors and handle them
- Use `context.Context` for request-scoped values, cancellation, deadlines

### Testing
- Use Go's built-in testing framework
- Write tests for all public functions and methods
- Use table-driven tests for multiple scenarios
- Use `mockgen` to generate mocks; use `go generate` directives

### Code Generation
- Use `go generate` for code generation tasks
- NEVER modify generated code manually

### Building
- When running `go build`, prefix output binary with `__tmp_`

### Reference
- When in doubt, consult https://go.dev/doc/effective_go
