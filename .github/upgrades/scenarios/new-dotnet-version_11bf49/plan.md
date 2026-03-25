# .NET 10.0 Upgrade Plan

## Table of Contents

- [Executive Summary](#executive-summary)
- [Migration Strategy](#migration-strategy)
- [Detailed Dependency Analysis](#detailed-dependency-analysis)
- [Project-by-Project Plans](#project-by-project-plans)
  - [DataFactoryLakehouse.csproj](#datafactorylakehousecsproj)
- [Risk Management](#risk-management)
- [Testing & Validation Strategy](#testing--validation-strategy)
- [Complexity & Effort Assessment](#complexity--effort-assessment)
- [Source Control Strategy](#source-control-strategy)
- [Success Criteria](#success-criteria)

---

## Executive Summary

### Scenario Description
Upgrade the DataFactoryLakehouse solution from .NET 9.0 to .NET 10.0 (Long Term Support). This is a single-project solution requiring framework retargeting, package updates, and API compatibility fixes.

### Scope
**Projects Affected**: 1 project
- DataFactoryLakehouse.csproj (currently net9.0 ? target net10.0)

**Current State**:
- Target Framework: net9.0
- 3 NuGet packages (2 require attention)
- 440 lines of code
- 5 code files (2 files with incidents)

**Target State**:
- Target Framework: net10.0
- Updated packages for .NET 10 compatibility
- API compatibility issues resolved
- Deprecated package addressed

### Selected Strategy
**All-At-Once Strategy** - Single atomic upgrade operation.

**Rationale**: 
- Single project (standalone application)
- No dependency coordination required
- Low complexity (440 LOC, ?? Low difficulty)
- Small impact surface (10+ LOC estimated changes)
- All packages have clear upgrade paths

### Discovered Metrics
- **Complexity Classification**: Simple
- **Total LOC**: 440
- **Estimated Impact**: 10+ LOC (2.3% of codebase)
- **Package Updates**: 1 upgrade, 1 deprecated
- **API Issues**: 1 binary incompatible, 8 source incompatible, 1 behavioral change
- **Risk Level**: Low

### Critical Issues
- **Deprecated Package**: Azure.Identity 1.16.0 (still compatible but deprecated)
- **Binary Incompatible API**: `Microsoft.Extensions.DependencyInjection.OptionsConfigurationServiceCollectionExtensions.Configure` requires code changes
- **Source Incompatible APIs**: `System.BinaryData` (7 occurrences) and `System.TimeSpan.FromSeconds` require attention
- **Behavioral Change**: `Microsoft.Extensions.Hosting.HostBuilder` behavior changes in .NET 10

### Recommended Approach
Single-phase atomic upgrade:
1. Update project file to net10.0
2. Update all packages simultaneously
3. Fix compilation errors from API changes
4. Build and validate
5. Test application functionality

### Iteration Strategy
Fast batch approach - all details completed in 2-3 iterations due to simple solution structure.

---

## Migration Strategy

### Approach Selection

**Selected Approach**: All-At-Once Strategy (Atomic Upgrade)

**Justification**:
1. **Single Project**: Only one project to upgrade - no coordination complexity
2. **Small Codebase**: 440 LOC with minimal impact (10+ lines estimated)
3. **Clear Package Path**: All packages have known versions or are compatible
4. **Low Risk Profile**: ?? Low difficulty rating, no security vulnerabilities
5. **No Dependencies**: Standalone project eliminates ordering concerns

### All-At-Once Strategy Rationale

The All-At-Once approach is ideal for this scenario because:

- **Atomic Operation**: All changes (framework, packages, API fixes) applied simultaneously
- **Single Testing Phase**: One comprehensive validation after all changes complete
- **Minimal Coordination**: No multi-targeting or phased dependency management
- **Fast Completion**: Shortest path to completion with unified changes
- **Simple Rollback**: Single commit can revert entire upgrade if needed

### Dependency-Based Ordering

Not applicable - single project with no inter-project dependencies.

**External Package Ordering**:
1. Update `Microsoft.Extensions.Hosting` to 10.0.5 (framework alignment)
2. Address `Azure.Identity` deprecation (evaluate replacement)
3. Keep `Azure.ResourceManager.DataFactory` at 1.9.0 (compatible)

### Execution Approach

**Sequential Steps** (all within single atomic operation):

1. **Framework Update**: Change `<TargetFramework>` from net9.0 to net10.0
2. **Package Updates**: Update `Microsoft.Extensions.Hosting` to 10.0.5
3. **Dependency Restore**: Run `dotnet restore`
4. **Build & Identify Issues**: Compile to surface API incompatibilities
5. **Fix Compilation Errors**: Address all API breaking changes:
   - Binary incompatible: `OptionsConfigurationServiceCollectionExtensions.Configure`
   - Source incompatible: `System.BinaryData` (7 occurrences), `System.TimeSpan.FromSeconds`
   - Behavioral: `HostBuilder` usage review
6. **Rebuild & Verify**: Confirm 0 errors, 0 warnings
7. **Runtime Testing**: Execute application to validate behavioral changes

### Risk Management Alignment

The All-At-Once approach minimizes risk through:
- **Clear Scope**: All changes visible in single review
- **Fast Feedback**: Immediate build/test results after changes
- **Simple Tracking**: One operation to monitor and validate
- **Easy Rollback**: Single commit reversion if critical issues arise

### Timeline

**Phase 0: Preparation** (if needed)
- Verify .NET 10 SDK installation
- Review deprecated package alternatives

**Phase 1: Atomic Upgrade** (single coordinated operation)
- Update project file (TargetFramework)
- Update package references
- Restore dependencies
- Build and fix all compilation errors
- Rebuild and verify success
- **Deliverable**: Solution builds with 0 errors

**Phase 2: Test Validation**
- Execute application
- Validate Azure Data Factory operations
- Verify hosting/configuration behavior
- Confirm no runtime regressions
- **Deliverable**: Application runs correctly

### Success Indicators

- ? Project file shows `<TargetFramework>net10.0</TargetFramework>`
- ? `Microsoft.Extensions.Hosting` at version 10.0.5
- ? All API incompatibilities resolved
- ? Solution builds without errors or warnings
- ? Application starts and runs without exceptions
- ? Azure Data Factory integration functions correctly

---

## Detailed Dependency Analysis

### Dependency Graph Summary

The solution consists of a single standalone project with no inter-project dependencies:

```
DataFactoryLakehouse.csproj (net9.0 ? net10.0)
??? No project dependencies
```

**Dependency Characteristics**:
- **Depth**: 0 (leaf node)
- **Dependencies**: 0 projects
- **Dependants**: 0 projects
- **External Dependencies**: 3 NuGet packages

### Project Groupings by Migration Phase

Since this is a single-project solution, there is only one migration phase:

**Phase 1: Atomic Upgrade**
- DataFactoryLakehouse.csproj

### Critical Path Identification

**Critical Path**: Direct upgrade (single node, no path complexity)

The upgrade is straightforward with no dependency coordination:
1. No prerequisite project upgrades required
2. No dependent projects to consider
3. All changes can be applied atomically

### External Package Dependencies

The project depends on 3 NuGet packages:

1. **Azure.Identity** 1.16.0
   - Status: ?? Deprecated
   - Action: Evaluate replacement or continue with deprecation notice
   - Impact: Authentication functionality

2. **Azure.ResourceManager.DataFactory** 1.9.0
   - Status: ? Compatible with .NET 10
   - Action: No change required (already compatible)
   - Impact: None

3. **Microsoft.Extensions.Hosting** 9.0.9
   - Status: ?? Upgrade recommended to 10.0.5
   - Action: Update to align with .NET 10
   - Impact: Host builder configuration

### Circular Dependencies

None detected - single project solution.

### Migration Order Justification

With only one project, the migration order is trivial. All changes (framework retargeting, package updates, API fixes) will be applied in a single coordinated operation.

---

## Project-by-Project Plans

## Project-by-Project Plans

### DataFactoryLakehouse.csproj

#### Project Overview

**Current State**:
- **Target Framework**: net9.0
- **Project Type**: DotNetCoreApp (SDK-style)
- **Lines of Code**: 440
- **Code Files**: 5 total (2 with incidents)
- **Dependencies**: 0 project dependencies
- **Dependants**: 0 projects depend on this
- **Risk Level**: ?? Low

**NuGet Packages**:
| Package | Current Version | Status |
|---------|----------------|--------|
| Azure.Identity | 1.16.0 | ?? Deprecated |
| Azure.ResourceManager.DataFactory | 1.9.0 | ? Compatible |
| Microsoft.Extensions.Hosting | 9.0.9 | ?? Upgrade to 10.0.5 |

**API Compatibility Issues**:
- 1 binary incompatible API
- 8 source incompatible APIs (7 `BinaryData`, 1 `TimeSpan`, 1 other)
- 1 behavioral change (`HostBuilder`)

**Target State**:
- **Target Framework**: net10.0
- **Updated Packages**: `Microsoft.Extensions.Hosting` 10.0.5
- **API Fixes Applied**: All 10 compatibility issues resolved
- **Deprecated Package**: `Azure.Identity` documented with plan for future replacement

---

#### Migration Steps

##### 1. Prerequisites

**Verify .NET 10 SDK Installation**:
```bash
dotnet --list-sdks
```
Ensure .NET 10.0.x SDK is installed. If not, download from: https://dotnet.microsoft.com/download/dotnet/10.0

**Check Current Project State**:
- Confirm current branch: `upgrade-to-NET10`
- Verify no pending changes (or commit them)
- Note current build status (should build successfully on net9.0)

---

##### 2. Framework Update

**Update Project File** (`DataFactoryLakehouse.csproj`):

Change the `TargetFramework` element:
```xml
<TargetFramework>net10.0</TargetFramework>
```

**File Location**: `DataFactoryLakehouse/DataFactoryLakehouse.csproj`

**Verification**: Inspect project file to confirm change

---

##### 3. Package Updates

**Update Package References** in `DataFactoryLakehouse.csproj`:

| Package | Current Version | Target Version | Reason |
|---------|----------------|----------------|--------|
| Microsoft.Extensions.Hosting | 9.0.9 | 10.0.5 | Framework alignment - required for .NET 10 compatibility |
| Azure.Identity | 1.16.0 | 1.16.0 | Keep current (deprecated but compatible) - document for future review |
| Azure.ResourceManager.DataFactory | 1.9.0 | 1.9.0 | No change needed (already compatible) |

**Update Method**:
```bash
dotnet add package Microsoft.Extensions.Hosting --version 10.0.5
```

Or manually edit `.csproj`:
```xml
<PackageReference Include="Microsoft.Extensions.Hosting" Version="10.0.5" />
```

**Restore Dependencies**:
```bash
dotnet restore DataFactoryLakehouse/DataFactoryLakehouse.csproj
```

**Verification**: Check that restore completes without errors

---

##### 4. Expected Breaking Changes

Based on the assessment, the following API compatibility issues will surface during compilation:

###### Binary Incompatible (1 issue)

**API**: `Microsoft.Extensions.DependencyInjection.OptionsConfigurationServiceCollectionExtensions.Configure<T>(IServiceCollection, IConfiguration)`

**Impact**: This method signature changed in .NET 10, requiring code modifications

**Expected Fix Pattern**:
- Review the calling code in affected files
- Update method signature to match new overload
- Possibly add explicit type parameters or change binding approach
- Consult: https://learn.microsoft.com/en-us/dotnet/core/compatibility/

**Files Potentially Affected**: Check files with dependency injection configuration

---

###### Source Incompatible (8 issues)

**API 1**: `System.BinaryData` (7 occurrences)

**Impact**: Type inference or compilation issues requiring explicit type annotations

**Expected Fix Pattern**:
- Add explicit type declarations where `BinaryData` is used
- Update implicit conversions to explicit
- Review serialization/deserialization patterns

**Files Affected**: 2 files with incidents (identified in assessment)

**Example Fix**:
```csharp
// Before (may fail in .NET 10)
var data = GetData();

// After (explicit type)
BinaryData data = GetData();
```

---

**API 2**: `System.TimeSpan.FromSeconds(System.Int64)` (1 occurrence)

**Impact**: Method signature changed or overload resolution issue

**Expected Fix Pattern**:
- Change `Int64` parameter to `double` if needed
- Use explicit cast: `TimeSpan.FromSeconds((double)longValue)`
- Or use alternative factory method

**Example Fix**:
```csharp
// Before
long seconds = 30;
var timeout = TimeSpan.FromSeconds(seconds);

// After
long seconds = 30;
var timeout = TimeSpan.FromSeconds((double)seconds);
```

---

###### Behavioral Change (1 issue)

**API**: `Microsoft.Extensions.Hosting.HostBuilder`

**Impact**: Hosting configuration and startup behavior may differ in .NET 10

**Expected Changes**:
- Default configuration order may have changed
- Dependency injection container initialization differences
- Middleware or service registration timing changes

**Mitigation**:
- Review `Program.cs` or hosting setup code
- Test application startup sequence
- Validate that services resolve correctly
- Confirm configuration values load as expected

**No Code Changes Expected** - but thorough testing required

---

##### 5. Code Modifications

**Step 5.1: Build to Identify Compilation Errors**

```bash
dotnet build DataFactoryLakehouse/DataFactoryLakehouse.csproj
```

**Expected Outcome**: Compilation errors from API incompatibilities

---

**Step 5.2: Fix Binary Incompatible API**

**File**: Locate file using `Configure<T>` method (check DI configuration)

**Action**:
1. Identify the exact usage of `OptionsConfigurationServiceCollectionExtensions.Configure<T>`
2. Review .NET 10 breaking change documentation
3. Update method call to match new signature
4. Add necessary type parameters or change to alternative configuration method

**Validation**: Code compiles without error for this API

---

**Step 5.3: Fix Source Incompatible APIs - BinaryData**

**Files**: 2 files with `BinaryData` usage (7 occurrences total)

**Action**:
1. Locate all `BinaryData` usages in affected files
2. Add explicit type declarations where compiler errors occur
3. Update any implicit conversions to explicit
4. Verify serialization/deserialization patterns work correctly

**Common Patterns**:
```csharp
// Pattern 1: Explicit type declaration
BinaryData data = BinaryData.FromString(jsonString);

// Pattern 2: Explicit conversion
var bytes = (byte[])binaryData;

// Pattern 3: Explicit construction
var data = new BinaryData(bytes);
```

**Validation**: All `BinaryData` compilation errors resolved

---

**Step 5.4: Fix Source Incompatible API - TimeSpan.FromSeconds**

**File**: Locate file with `TimeSpan.FromSeconds(Int64)` call

**Action**:
1. Find the `TimeSpan.FromSeconds` call with `Int64` parameter
2. Add explicit cast to `double`: `(double)longValue`
3. Or change parameter type to `double` if appropriate

**Validation**: `TimeSpan` compilation error resolved

---

**Step 5.5: Review Configuration Files**

**Files to Check**:
- `appsettings.json`
- `appsettings.Development.json`
- Any other configuration files

**Action**:
- No changes expected, but review for deprecated configuration keys
- Confirm configuration schema matches .NET 10 expectations

---

##### 6. Testing Strategy

**Step 6.1: Rebuild and Verify Zero Errors**

```bash
dotnet build DataFactoryLakehouse/DataFactoryLakehouse.csproj --no-incremental
```

**Success Criteria**:
- ? Build succeeds with 0 errors
- ? Build succeeds with 0 warnings (ideal)
- ? All API fixes applied correctly

---

**Step 6.2: Application Startup Test**

```bash
dotnet run --project DataFactoryLakehouse/DataFactoryLakehouse.csproj
```

**Validation Points**:
- ? Application starts without exceptions
- ? Hosting configuration loads correctly
- ? Dependency injection container resolves services
- ? Configuration binding works (settings load)
- ? Logging initializes properly

**Focus**: Verify `HostBuilder` behavioral change does not break startup

---

**Step 6.3: Azure Identity Authentication Test**

**Test**: Verify deprecated `Azure.Identity` package still functions

**Actions**:
- Execute any authentication flows
- Test Azure credential resolution
- Verify token acquisition works
- Confirm Azure Data Factory client authenticates

**Success Criteria**:
- ? Authentication succeeds
- ? No runtime exceptions from `Azure.Identity`
- ? Credentials resolve correctly

**Notes**: Document that package is deprecated for future replacement

---

**Step 6.4: Azure Data Factory Operations Test**

**Test**: Core Data Factory functionality

**Actions**:
- Test pipeline operations (list, trigger, monitor)
- Verify dataset operations
- Confirm linked service interactions
- Validate any custom Data Factory operations in code

**Success Criteria**:
- ? All Data Factory API calls succeed
- ? `Azure.ResourceManager.DataFactory` package works correctly
- ? No behavioral regressions in Data Factory operations

---

**Step 6.5: Functional Testing**

**Test**: End-to-end application scenarios

**Actions**:
- Execute primary application workflows
- Test error handling paths
- Verify logging and monitoring
- Confirm performance characteristics

**Success Criteria**:
- ? All functional scenarios pass
- ? No unexpected exceptions
- ? Performance is acceptable
- ? Error handling works correctly

---

##### 7. Validation Checklist

**Pre-Deployment Validation**:

- [ ] **Build Success**: `dotnet build` completes with 0 errors
- [ ] **No Warnings**: Build produces 0 warnings (or only expected warnings)
- [ ] **Framework Confirmed**: Project file shows `<TargetFramework>net10.0</TargetFramework>`
- [ ] **Packages Updated**: `Microsoft.Extensions.Hosting` at version 10.0.5
- [ ] **API Fixes Applied**: All 10 API compatibility issues resolved
- [ ] **Application Starts**: `dotnet run` succeeds without exceptions
- [ ] **Authentication Works**: Azure Identity authentication succeeds
- [ ] **Data Factory Works**: Azure Data Factory operations function correctly
- [ ] **No Runtime Errors**: Application runs without unexpected exceptions
- [ ] **Configuration Loads**: Settings bind correctly from `appsettings.json`
- [ ] **Behavioral Changes Tested**: `HostBuilder` behavior validated
- [ ] **Deprecated Package Documented**: `Azure.Identity` deprecation noted for future action

**Documentation**:

- [ ] **Code Comments**: Add comment noting `Azure.Identity` deprecation
- [ ] **Upgrade Notes**: Document any workarounds or non-obvious fixes
- [ ] **Breaking Changes**: Record any behavioral differences observed

**Source Control**:

- [ ] **Changes Committed**: All changes committed to `upgrade-to-NET10` branch
- [ ] **Commit Message**: Clear message describing .NET 10 upgrade
- [ ] **Ready for Review**: Branch ready for pull request/merge

---

#### Post-Migration Notes

**Deprecated Package - Azure.Identity**:
- Current version (1.16.0) is deprecated but remains functional
- No immediate action required for upgrade
- Plan future investigation of recommended replacement
- Monitor Azure SDK announcements for migration guidance
- Consider adding tracking issue for future package replacement

**Behavioral Change - HostBuilder**:
- Document any observed differences in hosting behavior
- Note any configuration changes made to accommodate .NET 10
- Record any workarounds applied

**Performance Monitoring**:
- Baseline performance after upgrade
- Monitor for any unexpected performance changes
- Compare startup time, memory usage, API call latency with .NET 9.0 baseline (if available)

---

## Risk Management

### High-Level Risk Assessment

**Overall Risk Level**: ?? **Low**

**Risk Factors**:
- Single project reduces coordination complexity
- Small codebase (440 LOC) limits impact surface
- Well-defined breaking changes with clear fixes
- No security vulnerabilities identified
- Deprecated package (`Azure.Identity`) still functional
- Low difficulty rating from assessment

### Risk Categories

| Risk Category | Level | Description | Mitigation |
|---------------|-------|-------------|------------|
| **API Breaking Changes** | ?? Medium | 1 binary incompatible, 8 source incompatible APIs | Follow documented fix patterns; test thoroughly |
| **Package Compatibility** | ?? Low | 1 upgrade, 1 deprecated, 1 compatible | Clear upgrade path; deprecated package still works |
| **Build Failures** | ?? Low | API changes will cause compilation errors | Fix errors methodically using assessment guidance |
| **Behavioral Changes** | ?? Medium | `HostBuilder` behavior changes in .NET 10 | Runtime testing to validate hosting configuration |
| **Deprecated Package** | ?? Low | `Azure.Identity` 1.16.0 deprecated | Continue using (still compatible) or plan replacement |
| **Data Loss** | ?? Low | No database/data migration required | N/A |
| **Deployment** | ?? Low | Single project, straightforward deployment | Ensure .NET 10 runtime on target environment |

### Specific Risks and Mitigation

#### Risk 1: Binary Incompatible API (High Impact)
**API**: `Microsoft.Extensions.DependencyInjection.OptionsConfigurationServiceCollectionExtensions.Configure<T>`

**Impact**: Code will not compile without changes

**Mitigation**:
- Review breaking change documentation for .NET 10
- Update method signature or usage pattern as documented
- Test configuration binding after changes

**Contingency**: If breaking change is complex, consider alternative configuration approaches

#### Risk 2: Source Incompatible APIs (Medium Impact)
**APIs**: 
- `System.BinaryData` (7 occurrences across 2 files)
- `System.TimeSpan.FromSeconds(System.Int64)`

**Impact**: Compilation errors requiring code updates

**Mitigation**:
- Identify all occurrences in affected files
- Apply fix patterns consistently
- Verify type compatibility after changes

**Contingency**: Use explicit type conversions or alternative APIs if direct fixes fail

#### Risk 3: Behavioral Change - HostBuilder (Medium Impact)
**API**: `Microsoft.Extensions.Hosting.HostBuilder`

**Impact**: Application hosting behavior may differ at runtime

**Mitigation**:
- Review .NET 10 hosting changes documentation
- Test application startup sequence
- Validate dependency injection container behavior
- Confirm configuration loading works correctly

**Contingency**: Adjust hosting configuration or revert to explicit builder patterns

#### Risk 4: Deprecated Package - Azure.Identity (Low Impact)
**Package**: `Azure.Identity` 1.16.0

**Impact**: Package is deprecated but still compatible; may lack future updates

**Mitigation**:
- Document deprecation in code comments
- Research recommended replacement (if any)
- Plan future migration if replacement available
- Continue using for now (still functional)

**Contingency**: Evaluate `Microsoft.Identity.*` alternatives or stay on current version

### Rollback Strategy

**Simple Rollback** (All-At-Once advantage):
1. Revert single commit containing all upgrade changes
2. Return to `main` branch
3. Solution immediately restored to .NET 9.0 state

**Conditions Triggering Rollback**:
- Critical runtime failures not resolvable within reasonable timeframe
- Showstopper performance degradation
- Blocking Azure Data Factory integration issues
- Unresolvable API breaking changes (unlikely given assessment)

### Risk Acceptance

**Accepted Risks**:
- **Deprecated Package**: `Azure.Identity` remains deprecated but functional; accepted for initial upgrade
- **Behavioral Changes**: `HostBuilder` changes accepted with thorough testing
- **Upgrade Timeline**: Minor productivity impact during upgrade (single project, minimal)

**Not Accepted** (must resolve):
- Any compilation errors preventing build
- Runtime exceptions on application startup
- Broken Azure Data Factory operations

---

## Testing & Validation Strategy

### Overview

The testing strategy for this single-project upgrade follows a layered approach: compilation validation, runtime validation, and functional validation. Since this is an All-At-Once upgrade, all testing occurs after the atomic upgrade operation completes.

---

### Phase-by-Phase Testing Requirements

#### Phase 1: Atomic Upgrade - Compilation Testing

**Objective**: Verify all code compiles successfully after framework and package updates

**Tests**:

1. **Clean Build Test**
   ```bash
   dotnet clean DataFactoryLakehouse/DataFactoryLakehouse.csproj
   dotnet build DataFactoryLakehouse/DataFactoryLakehouse.csproj
   ```
   **Expected**: 0 errors, 0 warnings (or only expected warnings)

2. **API Compatibility Verification**
   - All 10 API issues resolved
   - No compilation errors from breaking changes
   - Type inference works correctly

3. **Package Restoration Test**
   ```bash
   dotnet restore DataFactoryLakehouse/DataFactoryLakehouse.csproj
   ```
   **Expected**: All packages restore without conflicts

**Success Criteria**:
- ? Solution builds completely
- ? No API compatibility errors
- ? No package dependency conflicts
- ? All fixes applied correctly

**Failure Response**:
- If build fails: Review error messages, apply additional fixes, rebuild
- If package conflicts: Resolve version constraints, adjust package references
- Document any unexpected issues for risk reassessment

---

#### Phase 2: Runtime Validation - Startup & Configuration Testing

**Objective**: Verify application starts correctly and behaves as expected at runtime

**Tests**:

1. **Application Startup Test**
   ```bash
   dotnet run --project DataFactoryLakehouse/DataFactoryLakehouse.csproj
   ```
   **Expected**: Application starts without exceptions

   **Validation Points**:
   - ? Host builder initializes correctly (behavioral change verification)
   - ? Dependency injection container resolves all services
   - ? Configuration loads from `appsettings.json`
   - ? Logging system initializes
   - ? No startup exceptions or errors

2. **Configuration Binding Test**
   - Verify all configuration sections bind correctly
   - Check that environment-specific settings load
   - Confirm connection strings and Azure settings are accessible

3. **Dependency Injection Test**
   - Verify all registered services resolve
   - Check singleton/scoped/transient lifetimes work correctly
   - Confirm no circular dependency issues

**Success Criteria**:
- ? Application starts successfully
- ? Configuration loads correctly
- ? All services resolve from DI container
- ? No runtime exceptions during initialization

**Failure Response**:
- Review `HostBuilder` behavioral changes documentation
- Adjust configuration or service registration as needed
- Document any workarounds required

---

#### Phase 3: Functional Validation - Integration Testing

**Objective**: Verify core application functionality works correctly on .NET 10

**Tests**:

1. **Azure Identity Authentication Test**
   - Execute authentication flow using `Azure.Identity`
   - Verify credential resolution (DefaultAzureCredential, etc.)
   - Confirm token acquisition succeeds
   - Validate access to Azure resources

   **Notes**: Deprecated package testing - ensure it still functions despite deprecation

   **Success Criteria**:
   - ? Authentication succeeds
   - ? Azure credentials resolve correctly
   - ? No authentication-related exceptions

2. **Azure Data Factory Operations Test**
   - List Data Factory pipelines
   - Trigger pipeline execution (if applicable)
   - Monitor pipeline runs
   - Access datasets and linked services
   - Execute any custom Data Factory operations

   **Success Criteria**:
   - ? All Data Factory API calls succeed
   - ? Pipeline operations work correctly
   - ? No Data Factory integration issues
   - ? `Azure.ResourceManager.DataFactory` package functions properly

3. **Core Application Workflows**
   - Execute primary application scenarios
   - Test all major code paths
   - Verify error handling works correctly
   - Confirm logging captures expected information

   **Success Criteria**:
   - ? All workflows complete successfully
   - ? Error handling behaves correctly
   - ? Logging works as expected
   - ? No functional regressions

4. **Performance Baseline Test** (Optional but Recommended)
   - Measure application startup time
   - Record memory usage
   - Time critical operations (API calls, data processing)
   - Compare with .NET 9.0 baseline if available

   **Success Criteria**:
   - ? Performance is acceptable
   - ? No significant degradation vs .NET 9.0
   - ? Memory usage is reasonable

---

### Smoke Tests (Quick Validation)

After completing all upgrade steps, perform these quick checks:

**5-Minute Smoke Test**:
1. ? Build succeeds: `dotnet build`
2. ? Application starts: `dotnet run`
3. ? Authentication works: Execute auth flow
4. ? Data Factory accessible: List pipelines
5. ? No exceptions in logs: Check console output

**15-Minute Smoke Test** (before committing):
1. All above smoke tests
2. ? Execute primary workflow end-to-end
3. ? Verify configuration in Development environment
4. ? Test error handling (trigger expected error)
5. ? Review logs for warnings or unexpected messages

---

### Comprehensive Validation (Before Merge)

Before merging the `upgrade-to-NET10` branch, complete this full validation:

**Build & Compile**:
- [ ] Clean build succeeds with 0 errors
- [ ] No unexpected warnings
- [ ] All API fixes verified

**Runtime Initialization**:
- [ ] Application starts successfully
- [ ] Configuration loads correctly
- [ ] DI container resolves all services
- [ ] Logging initializes properly

**Azure Integration**:
- [ ] Azure Identity authentication succeeds
- [ ] Data Factory client initializes
- [ ] Data Factory operations work correctly
- [ ] No Azure SDK errors

**Functional Testing**:
- [ ] All primary workflows execute successfully
- [ ] Error handling works correctly
- [ ] No unexpected exceptions
- [ ] Logging captures expected information

**Behavioral Validation**:
- [ ] HostBuilder behavior verified
- [ ] No configuration loading issues
- [ ] Service resolution timing correct
- [ ] Middleware order maintained (if applicable)

**Documentation**:
- [ ] Deprecated package documented
- [ ] Behavioral changes noted
- [ ] Workarounds recorded (if any)

**Performance** (if baseline available):
- [ ] Startup time acceptable
- [ ] Memory usage reasonable
- [ ] API call latency comparable

---

### Test Environment Requirements

**Development Environment**:
- .NET 10.0 SDK installed
- Azure credentials configured (for authentication testing)
- Access to Azure Data Factory instance (or mock/dev instance)
- Configuration files properly set up

**Optional**:
- Staging environment with .NET 10 runtime
- Integration test environment for full end-to-end testing

---

### Failure Handling

**If Compilation Fails**:
1. Review error messages carefully
2. Consult .NET 10 breaking changes documentation
3. Apply additional fixes as needed
4. Rebuild and retest
5. Document any non-obvious fixes

**If Runtime Testing Fails**:
1. Check application logs for detailed error information
2. Review `HostBuilder` behavioral changes
3. Verify configuration files are correct
4. Adjust hosting configuration if needed
5. Retest after fixes

**If Functional Testing Fails**:
1. Isolate the failing scenario
2. Determine if issue is .NET 10-specific or pre-existing
3. Review related API breaking changes
4. Apply fix or workaround
5. Retest scenario

**If Critical Blocker Encountered**:
1. Document the issue thoroughly
2. Assess rollback necessity
3. Consult .NET 10 documentation and community
4. Consider alternative approaches
5. If unresolvable: Revert to .NET 9.0 and reassess strategy

---

### Success Metrics

**Upgrade is successful when**:
- ? All compilation tests pass
- ? All runtime tests pass
- ? All functional tests pass
- ? No regressions observed
- ? Performance is acceptable
- ? All validation checklists complete

**Ready for deployment when**:
- ? All success metrics met
- ? Code reviewed and approved
- ? Documentation updated
- ? Deprecated package noted for future action
- ? Rollback plan confirmed

---

## Complexity & Effort Assessment

### Overall Complexity Rating

**Solution Complexity**: ?? **Low**

**Justification**:
- Single project (no coordination overhead)
- Small codebase (440 LOC)
- Minimal estimated impact (10+ LOC, 2.3% of codebase)
- Clear breaking changes with documented fixes
- No dependency chain complications
- Assessment difficulty rating: ?? Low

### Project Complexity Breakdown

| Project | Complexity | LOC | Files with Issues | API Issues | Package Issues | Risk Level |
|---------|------------|-----|-------------------|------------|----------------|------------|
| DataFactoryLakehouse.csproj | ?? Low | 440 | 2 of 5 | 10 | 2 | ?? Low |

**DataFactoryLakehouse.csproj Complexity Factors**:
- **Code Size**: Small (440 LOC)
- **Issue Density**: 2 files affected out of 5 (40%)
- **API Changes**: 10 issues (1 binary, 8 source, 1 behavioral)
- **Package Updates**: 2 packages need attention
- **Framework Jump**: net9.0 ? net10.0 (single version increment)
- **External Integrations**: Azure Data Factory, Azure Identity

### Phase Complexity Assessment

**Phase 1: Atomic Upgrade**

| Task Category | Complexity | Effort | Notes |
|---------------|------------|--------|-------|
| Framework Update | ?? Low | Minimal | Single line change in project file |
| Package Updates | ?? Low | Minimal | 1 package upgrade (`Microsoft.Extensions.Hosting`) |
| Dependency Restore | ?? Low | Minimal | Automated via `dotnet restore` |
| Build & Identify Errors | ?? Low | Minimal | Automated compilation |
| Fix Binary Incompatible API | ?? Medium | Low-Medium | 1 API (`Configure<T>`), well-documented |
| Fix Source Incompatible APIs | ?? Medium | Low-Medium | 9 occurrences (7 `BinaryData`, 1 `TimeSpan`, 1 other) |
| Address Behavioral Change | ?? Low | Low | Review and test `HostBuilder` behavior |
| Address Deprecated Package | ?? Low | Low | Document or plan future replacement |
| Rebuild & Verify | ?? Low | Minimal | Automated compilation |

**Phase 2: Test Validation**

| Task Category | Complexity | Effort | Notes |
|---------------|------------|--------|-------|
| Application Startup | ?? Low | Low | Verify hosting configuration |
| Azure Identity Auth | ?? Medium | Low-Medium | Validate deprecated package still works |
| Data Factory Operations | ?? Medium | Medium | Test core functionality |
| Runtime Behavior | ?? Medium | Low-Medium | Confirm no behavioral regressions |

### Resource Requirements

**Skills Required**:
- .NET 10 framework knowledge (basic)
- C# compilation error resolution (intermediate)
- Azure SDK familiarity (intermediate)
- Hosting/configuration patterns (intermediate)

**Team Capacity**:
- **Single Developer**: Can complete entire upgrade (low complexity)
- **Parallel Work**: Not applicable (single project)
- **Review Requirements**: Standard code review for framework changes

### Effort Estimate (Relative Complexity)

**Complexity Distribution**:
- ?? **Low Complexity**: 70% of work (framework update, package updates, rebuild)
- ?? **Medium Complexity**: 30% of work (API fixes, behavioral testing)
- ?? **High Complexity**: 0%

**Relative Effort Ratings**:
- **Framework & Package Updates**: Low (straightforward, well-documented)
- **API Compatibility Fixes**: Low-Medium (10 issues, clear patterns)
- **Testing & Validation**: Low-Medium (functional testing of core operations)
- **Documentation**: Low (single project, minimal changes)

**Overall Assessment**: This is a straightforward upgrade suitable for a single developer. The All-At-Once approach minimizes overhead and enables rapid completion.

---

## Source Control Strategy

### Branching Strategy

**Current Setup**:
- **Main Branch**: `main` (source branch, .NET 9.0)
- **Upgrade Branch**: `upgrade-to-NET10` (target branch for all changes)
- **Pre-Upgrade State**: Committed before branch creation

**Branch Purpose**:
- `upgrade-to-NET10` contains all .NET 10 upgrade changes
- Isolated from `main` until fully tested and validated
- Enables easy rollback (return to `main` branch)
- Provides clear upgrade history

---

### Commit Strategy

**Recommended Approach**: **Single Commit** (All-At-Once Strategy alignment)

Since this is an atomic All-At-Once upgrade of a single project, all changes should be committed together as a logical unit. This provides:

- ? Clear upgrade boundary (one commit = entire upgrade)
- ? Simple rollback (revert single commit)
- ? Atomic history (all changes visible together)
- ? Easy code review (one cohesive change set)

**Single Commit Structure**:

```bash
git add .
git commit -m "Upgrade DataFactoryLakehouse to .NET 10.0

- Update TargetFramework from net9.0 to net10.0
- Upgrade Microsoft.Extensions.Hosting to 10.0.5
- Fix binary incompatible API: Configure<T> method
- Fix source incompatible APIs: BinaryData (7 occurrences), TimeSpan.FromSeconds
- Document Azure.Identity deprecation (package still functional)
- Validate HostBuilder behavioral changes
- All builds succeed with 0 errors
- All tests pass successfully"
```

**Commit Message Guidelines**:
- **First Line**: Clear summary of upgrade (50 chars or less)
- **Body**: Detailed list of changes
  - Framework update
  - Package updates
  - API fixes applied
  - Testing completed
- Include validation confirmation

---

### Alternative: Incremental Commits (If Preferred)

If you prefer incremental commits for checkpoint visibility:

**Commit 1: Framework and Package Updates**
```bash
git add DataFactoryLakehouse/DataFactoryLakehouse.csproj
git commit -m "Update DataFactoryLakehouse to .NET 10.0 and upgrade packages

- Change TargetFramework to net10.0
- Upgrade Microsoft.Extensions.Hosting to 10.0.5
- Keep Azure.Identity at 1.16.0 (deprecated but compatible)
- Keep Azure.ResourceManager.DataFactory at 1.9.0 (compatible)"
```

**Commit 2: API Compatibility Fixes**
```bash
git add <affected code files>
git commit -m "Fix .NET 10 API compatibility issues

- Fix binary incompatible API: Configure<T> method signature
- Fix source incompatible BinaryData usages (7 occurrences)
- Fix source incompatible TimeSpan.FromSeconds with Int64
- Add explicit type declarations where needed
- Update method signatures to match .NET 10 APIs"
```

**Commit 3: Documentation and Validation**
```bash
git add <any docs or comments>
git commit -m "Document .NET 10 upgrade and validate changes

- Add comments noting Azure.Identity deprecation
- Document HostBuilder behavioral changes
- Confirm all builds pass
- Validate all tests pass"
```

**Note**: While incremental commits provide checkpoints, they complicate rollback. Single commit is recommended for this small, atomic upgrade.

---

### Review and Merge Process

**Pull Request (PR) Requirements**:

**PR Title**: "Upgrade DataFactoryLakehouse to .NET 10.0"

**PR Description Template**:
```markdown
## Upgrade Summary
Upgrades DataFactoryLakehouse project from .NET 9.0 to .NET 10.0 (LTS)

## Changes Made
- ? Updated TargetFramework to net10.0
- ? Upgraded Microsoft.Extensions.Hosting to 10.0.5
- ? Fixed 1 binary incompatible API
- ? Fixed 8 source incompatible APIs
- ? Validated 1 behavioral change (HostBuilder)
- ? Documented Azure.Identity deprecation

## Testing Completed
- ? Build succeeds with 0 errors
- ? Application starts successfully
- ? Azure Identity authentication works
- ? Azure Data Factory operations validated
- ? All functional tests pass

## Known Issues / Follow-Up
- Azure.Identity 1.16.0 is deprecated but functional
- Plan future investigation of replacement package
- Monitor Azure SDK announcements for migration guidance

## Risk Assessment
?? Low Risk - Single project, small codebase, all tests passing

## Rollback Plan
Revert this PR or merge commit to return to .NET 9.0
```

**Review Checklist**:
- [ ] All commits have clear messages
- [ ] Project file changes reviewed
- [ ] Package updates validated
- [ ] API fixes reviewed for correctness
- [ ] Build succeeds in CI (if applicable)
- [ ] All tests pass
- [ ] Documentation updated
- [ ] No unintended changes included

**Merge Criteria**:
1. ? Code review approved
2. ? All CI/CD checks pass (if applicable)
3. ? All validation checklists complete
4. ? No merge conflicts with `main`
5. ? Deployment plan confirmed

**Merge Method**: 
- **Recommended**: Squash and merge (creates single commit on `main`)
- **Alternative**: Merge commit (preserves upgrade branch history)
- **Not Recommended**: Rebase and merge (rewrites history, complicates rollback)

---

### Post-Merge Actions

**After Merging to Main**:

1. **Tag the Upgrade**:
   ```bash
   git checkout main
   git pull
   git tag -a v1.0-net10.0 -m "Upgraded to .NET 10.0"
   git push origin v1.0-net10.0
   ```

2. **Clean Up Branch** (optional):
   ```bash
   git branch -d upgrade-to-NET10
   git push origin --delete upgrade-to-NET10
   ```

3. **Update Documentation**:
   - Update README with .NET 10 requirement
   - Update build instructions
   - Document deprecated packages

4. **Monitor Production** (if deployed):
   - Watch for runtime errors
   - Monitor performance metrics
   - Validate Azure integration in production

---

### Rollback Procedure

**If Critical Issue Found After Merge**:

**Option 1: Revert Merge Commit** (if squashed)
```bash
git checkout main
git revert <merge-commit-sha>
git push origin main
```

**Option 2: Revert to Previous Tag**
```bash
git checkout main
git reset --hard v1.0-net9.0  # Previous version tag
git push origin main --force  # Use with caution
```

**Option 3: Create Rollback Branch**
```bash
git checkout -b rollback-to-net9.0 <commit-before-merge>
# Create PR to merge rollback branch
```

**Rollback Testing**:
- ? Verify application builds on .NET 9.0
- ? Verify all functionality restored
- ? Confirm no data corruption
- ? Test critical workflows

---

### Git Best Practices for This Upgrade

1. **Commit Frequently** (during development): Make checkpoint commits while fixing APIs, but squash before final merge

2. **Write Clear Messages**: Explain *why* changes were made, not just *what* changed

3. **Review Before Committing**: Use `git diff` to verify only intended changes included

4. **Test Before Pushing**: Ensure build succeeds before pushing to remote

5. **Use Tags**: Tag stable milestones (pre-upgrade, post-upgrade) for easy reference

6. **Document Deprecated Packages**: Use code comments and commit messages to note deprecated package usage

---

### Collaboration Notes

**For Single Developer**:
- Single commit approach is efficient
- Self-review code changes before committing
- Use detailed commit message for future reference

**For Team Environment**:
- Consider incremental commits for transparency
- Request code review before merge
- Coordinate deployment timing
- Communicate deprecated package plan to team

**For CI/CD Pipeline**:
- Ensure pipeline supports .NET 10 SDK
- Update build agents/containers if needed
- Verify deployment environments have .NET 10 runtime
- Test CI/CD pipeline on upgrade branch before merge

---

## Success Criteria

### Technical Criteria

The .NET 10.0 upgrade is technically successful when:

#### Build Success
- ? **Clean Build**: `dotnet build` completes with 0 errors
- ? **No Warnings**: Build produces 0 warnings (or only documented expected warnings)
- ? **Target Framework**: Project file contains `<TargetFramework>net10.0</TargetFramework>`
- ? **All Files Compile**: All 5 code files compile successfully
- ? **No API Errors**: All 10 API compatibility issues resolved

#### Package Success
- ? **Package Updates Applied**: `Microsoft.Extensions.Hosting` upgraded to version 10.0.5
- ? **Compatible Packages Verified**: `Azure.ResourceManager.DataFactory` 1.9.0 works correctly
- ? **Deprecated Package Documented**: `Azure.Identity` 1.16.0 deprecation noted with plan
- ? **No Dependency Conflicts**: `dotnet restore` completes without version conflicts
- ? **No Security Vulnerabilities**: No new security issues introduced

#### Runtime Success
- ? **Application Starts**: `dotnet run` executes without exceptions
- ? **Host Builder Works**: Application hosting initializes correctly (behavioral change validated)
- ? **Configuration Loads**: Settings from `appsettings.json` bind correctly
- ? **DI Container Resolves**: All services resolve from dependency injection container
- ? **Logging Initializes**: Logging system starts without errors

#### Functional Success
- ? **Azure Authentication Works**: `Azure.Identity` package authenticates successfully
- ? **Data Factory Operations**: Azure Data Factory API calls succeed
- ? **Primary Workflows**: All core application scenarios execute correctly
- ? **Error Handling**: Exception handling and error logging work properly
- ? **No Runtime Exceptions**: Application runs without unexpected exceptions

#### API Compatibility Success
- ? **Binary Incompatible Fixed**: `Configure<T>` method updated correctly
- ? **Source Incompatible Fixed**: All `BinaryData` (7×) and `TimeSpan.FromSeconds` issues resolved
- ? **Behavioral Change Validated**: `HostBuilder` behavioral differences tested and confirmed

---

### Quality Criteria

The upgrade maintains code quality standards when:

#### Code Quality
- ? **Code Compiles Cleanly**: No compiler warnings or errors
- ? **Type Safety**: All type declarations explicit and correct
- ? **API Usage Correct**: All .NET 10 APIs used according to documentation
- ? **No Code Smells**: No temporary workarounds or "TODO" comments left unresolved
- ? **Consistent Style**: Code formatting and style maintained

#### Test Coverage
- ? **Build Tests Pass**: All compilation and build tests succeed
- ? **Startup Tests Pass**: Application initialization tests succeed
- ? **Integration Tests Pass**: Azure integration tests succeed
- ? **Functional Tests Pass**: End-to-end workflow tests succeed
- ? **Smoke Tests Pass**: Quick validation tests succeed

#### Documentation Quality
- ? **Deprecated Package Documented**: Code comments note `Azure.Identity` deprecation
- ? **Breaking Changes Documented**: Commit message lists all API fixes
- ? **Workarounds Documented**: Any non-obvious fixes explained in comments
- ? **README Updated**: Documentation reflects .NET 10 requirement (if applicable)
- ? **Upgrade Notes Created**: Plan and assessment files available for reference

---

### Process Criteria

The upgrade follows proper process when:

#### All-At-Once Strategy Adherence
- ? **Atomic Operation**: All changes (framework, packages, API fixes) completed together
- ? **Single Phase**: Upgrade completed in one coordinated operation
- ? **No Intermediate States**: No partial upgrades or multi-targeting
- ? **Unified Testing**: All validation performed after complete upgrade
- ? **Clear Boundaries**: Upgrade clearly delineated in source control

#### Source Control Success
- ? **Branch Strategy Followed**: All work on `upgrade-to-NET10` branch
- ? **Commits Clear**: Commit messages clearly describe changes
- ? **Changes Atomic**: Either single commit or logically grouped commits
- ? **Code Reviewed**: Changes reviewed before merge (if team environment)
- ? **Merge Successful**: Branch merged to `main` without conflicts

#### Validation Completeness
- ? **All Checklists Complete**: Every validation checklist item checked
- ? **Risk Assessment Confirmed**: All identified risks addressed or mitigated
- ? **Testing Strategy Executed**: All testing phases completed successfully
- ? **Smoke Tests Passed**: Quick validation tests confirm stability
- ? **Comprehensive Tests Passed**: Full validation tests confirm quality

---

### Deployment Criteria (If Applicable)

The upgrade is ready for deployment when:

#### Pre-Deployment
- ? **All Technical Criteria Met**: Build, package, runtime, functional success
- ? **All Quality Criteria Met**: Code quality, test coverage, documentation
- ? **All Process Criteria Met**: Strategy adherence, source control, validation
- ? **Deployment Plan Created**: Steps for deploying .NET 10 application documented
- ? **Rollback Plan Confirmed**: Procedure for reverting upgrade if needed

#### Deployment Environment
- ? **.NET 10 Runtime Available**: Target environment has .NET 10 installed
- ? **Azure Connectivity**: Application can authenticate and access Azure services
- ? **Configuration Updated**: Production configuration files compatible with .NET 10
- ? **Monitoring Ready**: Logging and monitoring configured to detect issues
- ? **Team Notified**: Stakeholders aware of upgrade deployment

#### Post-Deployment
- ? **Application Starts in Production**: Service starts without errors
- ? **Health Checks Pass**: Application health endpoints respond correctly
- ? **No Error Spikes**: Monitoring shows no unusual error rates
- ? **Performance Acceptable**: Response times and resource usage normal
- ? **Azure Operations Work**: Data Factory operations function in production

---

### Acceptance Criteria Summary

**Minimum Acceptance** (must meet all):
1. ? Solution builds with 0 errors
2. ? All API compatibility issues resolved
3. ? Application starts and runs without exceptions
4. ? Azure Identity and Data Factory integrations work
5. ? All validation checklists complete

**Full Acceptance** (ideal state):
1. ? All minimum acceptance criteria met
2. ? Build produces 0 warnings
3. ? All quality criteria met
4. ? All documentation updated
5. ? Code reviewed and approved
6. ? Deprecated package plan documented

**Deployment Acceptance** (if deploying):
1. ? All full acceptance criteria met
2. ? Deployment environment ready
3. ? Rollback plan confirmed
4. ? Monitoring configured
5. ? Team prepared for deployment

---

### Completion Checklist

Before considering the upgrade complete, verify:

**Technical Completion**:
- [ ] Project targets .NET 10.0
- [ ] All packages at correct versions
- [ ] All API issues fixed
- [ ] Build succeeds with 0 errors
- [ ] Application runs without exceptions

**Quality Completion**:
- [ ] Code quality maintained
- [ ] All tests pass
- [ ] Documentation updated
- [ ] No temporary workarounds remaining

**Process Completion**:
- [ ] All-At-Once strategy followed
- [ ] Source control strategy followed
- [ ] All validation checklists complete
- [ ] Code reviewed (if team environment)
- [ ] Changes merged to main branch

**Follow-Up Items Documented**:
- [ ] Azure.Identity deprecation noted
- [ ] Future package replacement planned
- [ ] Behavioral changes documented
- [ ] Performance baseline recorded (if taken)

---

### Definition of Done

**The .NET 10.0 upgrade is DONE when**:

1. ? All success criteria met (technical, quality, process)
2. ? All validation tests pass
3. ? Code merged to main branch
4. ? Documentation updated
5. ? Deprecated packages documented for future action
6. ? Upgrade artifacts (plan.md, assessment.md) archived
7. ? Team notified of completion
8. ? (Optional) Deployed to production and validated

**Post-Upgrade Actions**:
- Monitor application in production (if deployed)
- Track Azure.Identity deprecation announcements
- Plan future package updates
- Document lessons learned
- Update upgrade procedures based on experience
