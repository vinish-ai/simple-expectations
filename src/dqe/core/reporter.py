from dqe.core.models import ExpectationSuiteValidationResult
import sys

class TextReporter:
    def __init__(self, result: ExpectationSuiteValidationResult):
        self.result = result
        
    def print_report(self):
        print("\n" + "="*50)
        print(f"Validation Report: {self.result.suite_name}")
        print("="*50)
        
        status = "PASS" if self.result.success else "FAIL"
        print(f"Overall Status: {status}")
        print(f"Success Ratio:  {self.result.statistics['successful_expectations']} / {self.result.statistics['evaluated_expectations']} Expectations")
        
        warning_count = self.result.statistics.get('warning_expectations', 0)
        if warning_count > 0:
            print(f"Warnings:       {warning_count}")
        
        print("-"*50)
        
        for i, res in enumerate(self.result.results, 1):
            if res.success:
                icon = "✓"
                color = "\033[92m"
            elif res.severity == "warning":
                icon = "⚠"
                color = "\033[93m"
            else:
                icon = "✗"
                color = "\033[91m"
            reset = "\033[0m"
            
            severity_tag = f" [{res.severity.upper()}]" if res.severity == "warning" else ""
            print(f"{color}{icon}{reset} [{i}] {res.expectation_type}{severity_tag}")
            print(f"    Kwargs: {res.kwargs}")
            
            if not res.success:
                if res.exception_info:
                    print(f"    \033[91mError\033[0m:  {res.exception_info.get('error', 'Unknown Error')}")
                elif res.observed_value is not None:
                    print(f"    \033[93mObserved\033[0m: {res.observed_value}")
                    
                if res.unexpected_rows:
                    print(f"    \033[93mSample failing rows ({len(res.unexpected_rows)}):\033[0m")
                    for row in res.unexpected_rows[:5]:
                        print(f"      {row}")
                    if len(res.unexpected_rows) > 5:
                        print(f"      ... and {len(res.unexpected_rows) - 5} more")
            print()
            
        print("="*50 + "\n")
