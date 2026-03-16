from simple_expectations.core.models import ExpectationSuiteValidationResult
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
        print("-"*50)
        
        for i, res in enumerate(self.result.results, 1):
            check_mark = "✓" if res.success else "x"
            color = "\033[92m" if res.success else "\033[91m"
            reset = "\033[0m"
            
            print(f"{color}{check_mark}{reset} [{i}] {res.expectation_type}")
            print(f"    Kwargs: {res.kwargs}")
            
            if not res.success:
                if res.exception_info:
                    print(f"    \033[91mError\033[0m:  {res.exception_info.get('error', 'Unknown Error')}")
                elif res.observed_value is not None:
                    print(f"    \033[93mObserved\033[0m: {res.observed_value}")
            print()
            
        print("="*50 + "\n")
