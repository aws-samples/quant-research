#!/usr/bin/env python3
"""Feature Engineering Regression Test Suite."""
import os
import sys
import subprocess
from typing import List, Dict, Any

# Setup correct import path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, '..', '..')
sys.path.append(src_dir)

class RegressionTestSuite:
    """Regression test suite for feature engineering."""
    
    def __init__(self):
        self.test_dir = current_dir
        self.results = []
    
    def run_test(self, test_file: str, test_name: str) -> Dict[str, Any]:
        """Run a single test file and capture results."""
        test_path = os.path.join(self.test_dir, test_file)
        
        try:
            result = subprocess.run(
                [sys.executable, test_path],
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            success = result.returncode == 0
            output = result.stdout + result.stderr
            
            return {
                'test_name': test_name,
                'test_file': test_file,
                'success': success,
                'output': output,
                'return_code': result.returncode
            }
        except subprocess.TimeoutExpired:
            return {
                'test_name': test_name,
                'test_file': test_file,
                'success': False,
                'output': 'Test timed out after 5 minutes',
                'return_code': -1
            }
        except Exception as e:
            return {
                'test_name': test_name,
                'test_file': test_file,
                'success': False,
                'output': f'Error running test: {e}',
                'return_code': -2
            }
    
    def run_all_tests(self) -> List[Dict[str, Any]]:
        """Run all regression tests."""
        tests = [
            ('test_base.py', 'Base Feature Engineering Tests'),
            ('l2_feed/test_l2_bar_creation.py', 'L2Q Bar Creation Tests'),
            ('l2_feed/test_l2q_feature_accuracy.py', 'L2Q Sections 1&2 Feature Accuracy Tests'),
            ('trade_feed/test_aggregation_validation.py', 'Trade Aggregation Validation Tests')
        ]
        
        print("🚀 Starting Feature Engineering Regression Test Suite")
        print("=" * 60)
        
        for test_file, test_name in tests:
            print(f"\n📋 Running: {test_name}")
            print("-" * 40)
            
            result = self.run_test(test_file, test_name)
            self.results.append(result)
            
            if result['success']:
                print(f"✅ {test_name}: PASSED")
            else:
                print(f"❌ {test_name}: FAILED")
                print(f"Return code: {result['return_code']}")
            
            # Show key output lines
            output_lines = result['output'].split('\n')
            for line in output_lines:
                if any(marker in line for marker in ['✓', '⚠', '❌', '✅', '🎉']):
                    print(f"   {line}")
        
        return self.results
    
    def print_summary(self):
        """Print test summary."""
        print("\n" + "=" * 60)
        print("📊 REGRESSION TEST SUMMARY")
        print("=" * 60)
        
        passed = sum(1 for r in self.results if r['success'])
        total = len(self.results)
        
        print(f"Total Tests: {total}")
        print(f"Passed: {passed}")
        print(f"Failed: {total - passed}")
        
        if passed == total:
            print("\n🎉 ALL TESTS PASSED - No regression detected!")
        else:
            print(f"\n⚠️  {total - passed} TEST(S) FAILED - Regression detected!")
            print("\nFailed Tests:")
            for result in self.results:
                if not result['success']:
                    print(f"  ❌ {result['test_name']}")
        
        print("\nDetailed Results:")
        for result in self.results:
            status = "✅ PASS" if result['success'] else "❌ FAIL"
            print(f"  {status} - {result['test_name']}")

def main():
    """Run the regression test suite."""
    suite = RegressionTestSuite()
    suite.run_all_tests()
    suite.print_summary()
    
    # Exit with error code if any tests failed
    failed_count = sum(1 for r in suite.results if not r['success'])
    sys.exit(failed_count)

if __name__ == '__main__':
    main()