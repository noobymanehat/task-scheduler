package dev.scheduler.worker;

final class TestHandlers {

    private TestHandlers() {}

    interface Body {
        void run(JobContext context) throws Exception;
    }

    static JobHandler of(String type, Body body) {
        return new JobHandler() {
            @Override
            public String type() {
                return type;
            }

            @Override
            public void handle(JobContext context) throws Exception {
                body.run(context);
            }
        };
    }
}
