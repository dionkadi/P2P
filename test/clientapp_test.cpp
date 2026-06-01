#include "helper.hpp"

#include "ClientApp.hpp"

TEST(ClientAppShutdownTest, EmptyAppStopsPromptly) {
    ClientApp app;

    auto start = std::chrono::steady_clock::now();
    std::jthread stopper([&app] {
        std::this_thread::sleep_for(50ms);
        app.stop_all();
    });

    int rc = app.run();
    auto elapsed = std::chrono::steady_clock::now() - start;

    EXPECT_EQ(rc, 0);
    EXPECT_LT(elapsed, 2s);
}
