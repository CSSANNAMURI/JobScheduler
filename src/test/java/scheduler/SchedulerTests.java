package scheduler;

/** Plain-Java test runner (no JUnit needed). Run: sh run-tests.sh */
public class SchedulerTests {

    /** Entry point: run every suite and exit 0 on success, 1 on failure. */
    public static void main(String[] args) {
        try {
            runAll();
            System.exit(0);
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(1);
        }
    }

    /** Reset the pass counter and run all test suites in order. */
    static void runAll() {
        TestSupport.passed = 0;
        TokenBucketTests.run();
        AssignmentExamplesTests.run();
        SchedulerCoreTests.run();
        RealClockRegressionTests.run();
        SchedulerConcurrencyTests.run();
        TenantCleanupTests.run();
        TypedApiTests.run();
        System.out.println("\nAll " + TestSupport.passed + " tests passed.");
    }
}
