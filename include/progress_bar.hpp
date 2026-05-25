#pragma once

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cmath>
#include <concepts>
#include <cstddef>
#include <format>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <ranges>
#include <sstream>
#include <string>
#include <cstring>
#include <string_view>
#include <thread>
#include <type_traits>
#include <variant>
#include <vector>
#include <deque>
#include <iomanip> // For std::put_time

#ifdef _WIN32
#include <windows.h>
#else
#include <sys/ioctl.h>
#include <termios.h>
#include <unistd.h>
#include <signal.h>
#endif

namespace progressbar {

template<typename T>
concept Numeric = std::integral<T> || std::floating_point<T>;

template<typename T>
struct is_atomic_numeric : std::false_type {};

template<typename U>
struct is_atomic_numeric<std::atomic<U>>
    : std::bool_constant<Numeric<U>> {};

template<typename T>
concept AtomicNumeric = is_atomic_numeric<std::remove_cvref_t<T>>::value;

template<typename T>
concept NumericLike =
    Numeric<T> || AtomicNumeric<T>;

template<typename T>
concept InvocableReturning = requires(T t) {
    { t() } -> std::convertible_to<typename std::invoke_result_t<T>::value_type>;
};

template<typename T>
concept Stringable = requires(T t) {
    { std::to_string(t) } -> std::convertible_to<std::string>;
};

template<typename T>
concept Streamable = requires(std::ostream& os, T t) {
    { os << t } -> std::convertible_to<std::ostream&>;
};

namespace style {
    // Basic colors
    inline constexpr std::string_view black = "\033[30m";
    inline constexpr std::string_view red = "\033[31m";
    inline constexpr std::string_view green = "\033[32m";
    inline constexpr std::string_view yellow = "\033[33m";
    inline constexpr std::string_view blue = "\033[34m";
    inline constexpr std::string_view magenta = "\033[35m";
    inline constexpr std::string_view cyan = "\033[36m";
    inline constexpr std::string_view white = "\033[37m";
    
    // Bright colors
    inline constexpr std::string_view bright_black = "\033[90m";
    inline constexpr std::string_view bright_red = "\033[91m";
    inline constexpr std::string_view bright_green = "\033[92m";
    inline constexpr std::string_view bright_yellow = "\033[93m";
    inline constexpr std::string_view bright_blue = "\033[94m";
    inline constexpr std::string_view bright_magenta = "\033[95m";
    inline constexpr std::string_view bright_cyan = "\033[96m";
    inline constexpr std::string_view bright_white = "\033[97m";
    
    // Background colors
    inline constexpr std::string_view bg_black = "\033[40m";
    inline constexpr std::string_view bg_red = "\033[41m";
    inline constexpr std::string_view bg_green = "\033[42m";
    inline constexpr std::string_view bg_yellow = "\033[43m";
    inline constexpr std::string_view bg_blue = "\033[44m";
    inline constexpr std::string_view bg_magenta = "\033[45m";
    inline constexpr std::string_view bg_cyan = "\033[46m";
    inline constexpr std::string_view bg_white = "\033[47m";
    
    // Text styles
    inline constexpr std::string_view bold = "\033[1m";
    inline constexpr std::string_view dim = "\033[2m";
    inline constexpr std::string_view italic = "\033[3m";
    inline constexpr std::string_view underline = "\033[4m";
    inline constexpr std::string_view blink = "\033[5m";
    inline constexpr std::string_view reverse = "\033[7m";
    inline constexpr std::string_view hidden = "\033[8m";
    inline constexpr std::string_view strikethrough = "\033[9m";
    
    // Reset
    inline constexpr std::string_view reset = "\033[0m";
    inline constexpr std::string_view reset_bold = "\033[22m";
    inline constexpr std::string_view reset_dim = "\033[22m";
    inline constexpr std::string_view reset_italic = "\033[23m";
    inline constexpr std::string_view reset_underline = "\033[24m";
    inline constexpr std::string_view reset_blink = "\033[25m";
    inline constexpr std::string_view reset_reverse = "\033[27m";
    inline constexpr std::string_view reset_hidden = "\033[28m";
    inline constexpr std::string_view reset_strikethrough = "\033[29m";
}

/// RGB color for true color support
struct RGB {
    uint8_t r, g, b;
    
    constexpr RGB(uint8_t red, uint8_t green, uint8_t blue) noexcept 
        : r(red), g(green), b(blue) {}
    
    constexpr bool operator==(const RGB& other) const noexcept {
        return r == other.r && g == other.g && b == other.b;
    }

    [[nodiscard]] std::string to_ansi_foreground() const {
        return std::format("\033[38;2;{};{};{}m", r, g, b);
    }
    
    [[nodiscard]] std::string to_ansi_background() const {
        return std::format("\033[48;2;{};{};{}m", r, g, b);
    }
};

/// Predefined RGB colors
namespace rgb {
    inline const RGB orange{255, 165, 0};
    inline const RGB pink{255, 192, 203};
    inline const RGB purple{128, 0, 128};
    inline const RGB brown{165, 42, 42};
    inline const RGB gold{255, 215, 0};
    inline const RGB silver{192, 192, 192};
    inline const RGB coral{255, 127, 80};
    inline const RGB teal{0, 128, 128};
    inline const RGB lavender{230, 230, 250};
}

/// Gradient generator for progress bars
class Gradient {
    std::vector<RGB> stops_;
    
public:
    Gradient(std::initializer_list<RGB> colors) : stops_{colors} {}
    
    [[nodiscard]] RGB at(double t) const noexcept {
        t = std::clamp(t, 0.0, 1.0);
        if (stops_.size() == 1) return stops_[0];
        
        double segment = t * (stops_.size() - 1);
        size_t idx = static_cast<size_t>(segment);
        double frac = segment - idx;
        
        if (idx >= stops_.size() - 1) return stops_.back();
        
        const RGB& start = stops_[idx];
        const RGB& end = stops_[idx + 1];
        
        return RGB{
            static_cast<uint8_t>(start.r + (end.r - start.r) * frac),
            static_cast<uint8_t>(start.g + (end.g - start.g) * frac),
            static_cast<uint8_t>(start.b + (end.b - start.b) * frac)
        };
    }
};

class Terminal {
private:
    static std::once_flag flag_;
    static std::atomic<bool> initialized_;
    static std::atomic<size_t> width_;
    static std::atomic<size_t> height_;
    
    static void init() noexcept {
        if (initialized_.load(std::memory_order_acquire)) return;
        
        std::call_once(flag_, []{
            update_size();
#ifndef _WIN32
            // Setup SIGWINCH handler for terminal resize
            struct sigaction sa;
            sa.sa_handler = [](int) { update_size(); };
            sigemptyset(&sa.sa_mask);
            sa.sa_flags = 0;
            sigaction(SIGWINCH, &sa, nullptr);
#endif
            initialized_.store(true, std::memory_order_release);
        });
    }
    
public:
    [[nodiscard]] static std::pair<size_t, size_t> size() noexcept {
        init();
        return {width_.load(), height_.load()};
    }
    
    [[nodiscard]] static size_t width() noexcept {
        init();
        return width_.load();
    }
    
    [[nodiscard]] static size_t height() noexcept {
        init();
        return height_.load();
    }
    
    static void update_size() noexcept {
#ifdef _WIN32
        CONSOLE_SCREEN_BUFFER_INFO csbi;
        if (GetConsoleScreenBufferInfo(GetStdHandle(STD_OUTPUT_HANDLE), &csbi)) {
            width_.store(csbi.srWindow.Right - csbi.srWindow.Left + 1);
            height_.store(csbi.srWindow.Bottom - csbi.srWindow.Top + 1);
        }
#else
        winsize w;
        if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &w) == 0) {
            width_.store(w.ws_col);
            height_.store(w.ws_row);
        }
#endif
    }
    
    [[nodiscard]] static bool supports_color() noexcept {
#ifdef _WIN32
        // Windows 10+ supports ANSI escape codes
        return true; // Modern Windows terminals support this
#else
        const char* term = std::getenv("TERM");
        return term && (std::strstr(term, "xterm") || 
                        std::strstr(term, "color") ||
                        std::strstr(term, "ansi") ||
                        std::strstr(term, "linux"));

#endif
    }
    
    [[nodiscard]] static bool supports_true_color() noexcept {
        const char* colorterm = std::getenv("COLORTERM");
        return colorterm && (std::strstr(colorterm, "truecolor") ||
                            std::strstr(colorterm, "24bit"));
    }
    
    static void clear_line() noexcept {
        std::cout << "\033[2K\r";
    }
    
    static void move_to_line(size_t line) noexcept {
        std::cout << "\033[" << line << ";1H";
    }
    
    static void hide_cursor() noexcept {
        std::cout << "\033[?25l";
    }
    
    static void show_cursor() noexcept {
        std::cout << "\033[?25h";
    }
    
    static void alternate_screen() noexcept {
        std::cout << "\033[?1049h";
    }
    
    static void main_screen() noexcept {
        std::cout << "\033[?1049l";
    }
};

inline std::atomic<bool> Terminal::initialized_{false};
inline std::atomic<size_t> Terminal::width_{80};
inline std::atomic<size_t> Terminal::height_{24};
inline std::once_flag Terminal::flag_;

struct UnicodeRange {
    uint32_t end;
    uint8_t width;
};

constexpr std::array<UnicodeRange, 2> zero_width_ranges = {{
    {0x0000, 0x001F}, {0x007F, 0x009F}
}};

// Sorted by end code point for upper_bound search
constexpr std::array<UnicodeRange, 12> unicode_width_table = {{
    {0x001F, 0}, {0x007E, 1}, {0x009F, 0}, {0x206F, 1},
    {0x259F, 1}, {0x2BFF, 1}, {0x27BF, 2}, {0x2E7F, 1},
    {0xA4CF, 2}, {0xD7A3, 2}, {0xFAFF, 2}, {0x2FA1F, 2}
}};

// Function to decode a UTF-8 character and return its codepoint and byte length
// Returns 0 if invalid or continuation byte, updates `length`
inline uint32_t decode_utf8(std::string_view s, size_t& byte_length) {
    if (s.empty()) {
        byte_length = 0;
        return 0;
    }
    unsigned char c = static_cast<unsigned char>(s[0]);
    if ((c & 0x80) == 0) { // 1-byte character (ASCII)
        byte_length = 1;
        return c;
    } else if ((c & 0xE0) == 0xC0) { // 2-byte character
        if (s.length() < 2) { byte_length = 0; return 0; } // Incomplete sequence
        byte_length = 2;
        return ((uint32_t)(c & 0x1F) << 6) | (s[1] & 0x3F);
    } else if ((c & 0xF0) == 0xE0) { // 3-byte character
        if (s.length() < 3) { byte_length = 0; return 0; } // Incomplete sequence
        byte_length = 3;
        return ((uint32_t)(c & 0x0F) << 12) | ((uint32_t)(s[1] & 0x3F) << 6) | (s[2] & 0x3F);
    } else if ((c & 0xF8) == 0xF0) { // 4-byte character
        if (s.length() < 4) { byte_length = 0; return 0; } // Incomplete sequence
        byte_length = 4;
        return ((uint32_t)(c & 0x07) << 18) | ((uint32_t)(s[1] & 0x3F) << 12) |
               ((uint32_t)(s[2] & 0x3F) << 6) | (s[3] & 0x3F);
    }
    byte_length = 0; // Invalid byte or continuation byte
    return 0;
}

// Helper to check if a codepoint falls within a sorted array of ranges
constexpr bool is_in_ranges(uint32_t cp, std::span<const std::pair<uint32_t, uint32_t>> ranges) noexcept {
    auto it = std::ranges::upper_bound(ranges, cp, {}, &std::pair<uint32_t, uint32_t>::first);
    if (it == ranges.begin()) return false;
    --it;
    return cp <= it->second;
}

inline int get_character_display_width(uint32_t codepoint) noexcept {
    // ASCII Fast Path (Eliminates binary search overhead for standard text)
    if (codepoint < 0x80) [[likely]] {
        return (codepoint >= 0x20 && codepoint != 0x7F) ? 1 : 0;
    }

    // 0-width (Control characters and Zero-Width spaces)
    static constexpr auto zero_width = std::to_array<std::pair<uint32_t, uint32_t>>({
        {0x0000, 0x001F}, {0x007F, 0x009F}, {0x0300, 0x036F}, // Combining Diacritical Marks
        {0x200B, 0x200F}, {0x2028, 0x202E}, {0xFEFF, 0xFEFF}
    });

    // 2-width (East Asian Wide, Emojis, etc.)
    static constexpr auto double_width = std::to_array<std::pair<uint32_t, uint32_t>>({
        {0x1100, 0x11FF}, {0x2329, 0x232A}, {0x2E80, 0x303E},
        {0x3040, 0xA4CF}, {0xAC00, 0xD7A3}, {0xF900, 0xFAFF},
        {0xFE10, 0xFE19}, {0xFE30, 0xFE6F}, {0xFF00, 0xFF60},
        {0xFFE0, 0xFFE6}, {0x1F300, 0x1F6FF}, {0x1F900, 0x1F9FF},
        {0x20000, 0x2FFFD}, {0x30000, 0x3FFFD}
    });

    if (is_in_ranges(codepoint, zero_width)) return 0;
    if (is_in_ranges(codepoint, double_width)) return 2;
    
    return 1; // Default to 1 for basic ASCII and unlisted printable chars
}

// Helper to count the total display width of a string, handling ANSI escape codes and multi-cell UTF-8 characters
inline size_t count_visible_characters(std::string_view s) {
    size_t width = 0;
    bool in_escape = false;
    size_t i = 0;
    while (i < s.length()) {
        if (s[i] == '\033') {
            in_escape = true;
            i++;
            continue;
        }
        if (in_escape) {
            if (s[i] == 'm') in_escape = false; // End of SGR sequence
            i++;
            continue;
        }
        size_t char_byte_length = 0;
        uint32_t codepoint = decode_utf8(s.substr(i), char_byte_length);
        if (char_byte_length == 0) { // Invalid UTF-8 or unexpected byte
            i++; // Advance by 1 byte to avoid infinite loop
            continue;
        }
        
        width += get_character_display_width(codepoint);
        i += char_byte_length;
    }
    return width;
}

inline std::string truncate(const std::string& s, size_t max_visible_len) {
    std::string result;
    size_t current_visible_width = 0;
    bool in_escape = false;
    size_t i = 0;
    while (i < s.length()) {
        char c = s[i];
        
        if (c == '\033') {
            in_escape = true;
            result += c;
            i++;
            continue;
        }
        if (in_escape) {
            result += c;
            if (c == 'm') in_escape = false;
            i++;
            continue;
        }
        
        size_t char_byte_length = 0;
        uint32_t codepoint = decode_utf8(s.substr(i), char_byte_length);
        
        if (char_byte_length == 0) { // Invalid char or continuation byte
            // Skip problematic byte to prevent infinite loop
            i++; 
            continue;
        }
        
        int char_width = get_character_display_width(codepoint);
        
        if (current_visible_width + char_width > max_visible_len) {
            // If the next character would exceed the limit, stop here.
            break;
        }
        
        // Append the character (and its continuation bytes)
        result.append(s.data() + i, char_byte_length);
        current_visible_width += char_width;
        i += char_byte_length;
    }
    return result;
}

constexpr std::string_view to_string_view(std::u8string_view u8sv) noexcept {
    return std::string_view {
        reinterpret_cast<const char*>(u8sv.data()),
        u8sv.size()
    };
}

constexpr std::string to_string(std::u8string_view u8sv) noexcept {
    return std::string {
        reinterpret_cast<const char*>(u8sv.data()),
        u8sv.size()
    };
}

constexpr std::string repeat_view(size_t count, std::u8string_view u8sv) noexcept {
    std::string out;
    out.reserve(count * u8sv.size());
    std::string_view sv {
        reinterpret_cast<const char*>(u8sv.data()),
        u8sv.size()
    };
    for (auto _ : std::views::iota(0u, count)) {
        out += sv;
    }
    return out;
}

class Text {
    std::string text_;
    std::string ansi_prefix_;
    size_t visible_len_;

    mutable std::string cached_str_;
    mutable bool dirty_str_ = true;

    void update_length() {
        visible_len_ = count_visible_characters(text_);
        dirty_str_ = true;
    }
    
public:
    Text(std::string text = "") 
        : text_{std::move(text)} 
    {
        update_length();
    }

    Text(const char* text) : Text{std::string{text}} {}
    
    Text& style(std::string_view style_code) {
        ansi_prefix_.append(style_code);
        dirty_str_ = true;
        return *this;
    }
    
    Text& bold() { return style(style::bold); }
    Text& italic() { return style(style::italic); }
    Text& underline() { return style(style::underline); }
    Text& strikethrough() { return style(style::strikethrough); }
    
    Text& color(std::string_view color_code) { return style(color_code); }
    Text& bg(std::string_view bg_code) { return style(bg_code); }
    
    Text& rgb(const RGB& color) {
        if (Terminal::supports_true_color()) {
            ansi_prefix_.append(color.to_ansi_foreground());
        }
        dirty_str_ = true;
        return *this;
    }
    
    Text& bg_rgb(const RGB& color) {
        if (Terminal::supports_true_color()) {
            ansi_prefix_.append(color.to_ansi_background());
        }
        dirty_str_ = true;
        return *this;
    }
    
    [[nodiscard]] const std::string& str() const {
        if (!dirty_str_) return cached_str_;

        if (!Terminal::supports_color() || ansi_prefix_.empty()) {
            cached_str_ = text_;
        } else {
            cached_str_.clear();
            cached_str_.reserve(ansi_prefix_.size() + text_.size() + style::reset.size());
            cached_str_.append(ansi_prefix_).append(text_).append(style::reset);
        }
        
        dirty_str_ = false;
        return cached_str_;
    }

    void append_to(std::string& out) const {
        out.append(str());
    }
    
    [[nodiscard]] size_t visible_length() const noexcept {
        return visible_len_;
    }
    
    friend std::ostream& operator<<(std::ostream& os, const Text& text) {
        return os << text.str();
    }
};

class Markdown {
public:
    static Text parse(std::string_view markdown) {
        std::string final_str;
        final_str.reserve(markdown.size() + 128); // Generous pre-allocation for ANSI codes
        
        bool is_bold = false, is_italic = false, is_under = false, is_strike = false, is_code = false;
        
        auto apply_styles = [&]() {
            final_str.append(style::reset);
            if (is_bold) final_str.append(style::bold);
            if (is_italic) final_str.append(style::italic);
            if (is_under) final_str.append(style::underline);
            if (is_strike) final_str.append(style::strikethrough);
            if (is_code) final_str.append(style::bright_cyan);
        };
        
        size_t i = 0;
        while (i < markdown.size()) {
            std::string_view remaining = markdown.substr(i);
            
            if (remaining.starts_with("```")) {
                is_code = !is_code; apply_styles(); i += 3;
            } else if (!is_code && remaining.starts_with("**")) {
                is_bold = !is_bold; apply_styles(); i += 2;
            } else if (!is_code && remaining.starts_with("__")) {
                is_under = !is_under; apply_styles(); i += 2;
            } else if (!is_code && remaining.starts_with("~~")) {
                is_strike = !is_strike; apply_styles(); i += 2;
            } else if (!is_code && (remaining.starts_with("*") || remaining.starts_with("_"))) {
                is_italic = !is_italic; apply_styles(); i += 1;
            } else {
                final_str.push_back(markdown[i++]);
            }
        }
        
        if (is_bold || is_italic || is_under || is_strike || is_code) {
            final_str.append(style::reset);
        }
        
        return Text{std::move(final_str)};
    }
};

class Layout {
public:
    enum class Justify {
        Left,
        Center,
        Right,
        SpaceBetween
    };
    
    enum class Overflow {
        Clip,
        Ellipsis,
        Wrap
    };
    
    struct Config {
        size_t width = 0;
        Justify justify = Justify::Left;
        Overflow overflow = Overflow::Ellipsis;
        size_t padding_left = 0;
        size_t padding_right = 0;
        std::optional<size_t> max_lines;
    };

    static void append_justified(
        std::string& out, 
        std::string_view text, 
        size_t visible_len, 
        size_t width, 
        Justify justify
    ) {
        if (visible_len >= width) {
            out.append(truncate(std::string(text), width));
            return;
        }
        
        size_t padding = width - visible_len;
        switch (justify) {
            case Justify::Left:
                out.append(text);
                out.append(padding, ' ');
                break;
            case Justify::Right:
                out.append(padding, ' ');
                out.append(text);
                break;
            case Justify::Center: {
                size_t left = padding / 2;
                size_t right = padding - left;
                out.append(left, ' ');
                out.append(text);
                out.append(right, ' ');
                break;
            }
            case Justify::SpaceBetween:
                out.append(text);
                out.append(padding, ' '); // Fallback to Left for single-line SpaceBetween
                break;
        }
    }
    
    [[nodiscard]] static std::string justify_text(
        std::string_view text, 
        size_t width, 
        Justify justify,
        std::optional<size_t> known_vis_len = std::nullopt
    ) {
        if (text.empty()) {
            return "";
        }

        size_t visible_len = known_vis_len.value_or(Text{std::string(text)}.visible_length());
        
        if (visible_len >= width) return truncate(std::string(text), width);
        
        std::string out;
        out.reserve(width + text.size());
        append_justified(out, text, visible_len, width, justify);
        return out;
    }
    
    [[nodiscard]] static std::vector<std::string> wrap_text(
        std::string_view text, 
        size_t width
    ) {
        std::vector<std::string> lines;
        std::string current_line;
        size_t current_width = 0;
        
        std::stringstream ss{std::string(text)};
        std::string word;
        
        while (ss >> word) {
            Text word_text{word};
            size_t word_width = word_text.visible_length();
            
            if (current_width + word_width + (current_line.empty() ? 0 : 1) <= width) {
                if (!current_line.empty()) {
                    current_line += ' ';
                    current_width++;
                }
                current_line += word;
                current_width += word_width;
            } else {
                if (!current_line.empty()) {
                    lines.push_back(current_line);
                }
                current_line = word;
                current_width = word_width;
            }
        }
        
        if (!current_line.empty()) {
            lines.push_back(current_line);
        }
        
        return lines;
    }
};

class Table {
public:
    struct Column {
        std::string header;
        Layout::Justify justify = Layout::Justify::Left;
        size_t min_width = 0;
        size_t max_width = 0;
        bool expand = false;
        size_t cached_header_width = 0;
    };
    
    struct Cell {
        Text content;
        size_t cached_visible_width = 0;
        std::optional<std::string_view> color;
        std::optional<std::string_view> style;

        Cell(std::string s) : content(std::move(s)) { cached_visible_width = content.visible_length(); }
        Cell(const char *s) : content(std::string(s)) { cached_visible_width = content.visible_length(); }
        Cell(Text t) : content(std::move(t)) { cached_visible_width = content.visible_length(); }
    };
    
private:
    std::vector<Column> columns_;
    std::vector<std::vector<Cell>> rows_;
    std::vector<size_t> column_widths_;
    std::string title_;
    bool show_header_ = true;
    bool show_borders_ = true;
    std::vector<std::u8string_view> border_style_ = {u8"─", u8"│", u8"┌", u8"┐", u8"└", u8"┘", u8"├", u8"┤", u8"┬", u8"┴", u8"┼"};
    
    static void append_repeated(std::string& out, std::string_view sv, size_t count) {
        out.reserve(out.size() + sv.size() * count);
        for (size_t i = 0; i < count; ++i) out.append(sv);
    }

public:
    Table() = default;
    
    Table& add_column(Column col) {
        col.cached_header_width = Text{col.header}.visible_length();
        columns_.push_back(std::move(col));
        return *this;
    }
    
    Table& add_column(std::string header, Layout::Justify justify = Layout::Justify::Left) {
        size_t hw = Text{header}.visible_length();
        columns_.push_back(Column{std::move(header), justify, 0, 0, false, hw});
        return *this;
    }
    
    Table& add_row(std::vector<Cell> row) {
        rows_.push_back(std::move(row));
        return *this;
    }
    
    template<typename... Cells>
    Table& add_row(Cells&&... cells) {
        rows_.push_back({Cell{std::forward<Cells>(cells)}...});
        return *this;
    }
    
    Table& title(std::string title) {
        title_ = std::move(title);
        return *this;
    }
    
    Table& show_header(bool show) {
        show_header_ = show;
        return *this;
    }
    
    Table& show_borders(bool show) {
        show_borders_ = show;
        return *this;
    }
    
    [[nodiscard]] std::string render() const {
        if (columns_.empty()) return "";
        
        // Calculate column widths
        std::vector<size_t> widths(columns_.size(), 0);
        
        // Header widths
        for (size_t i = 0; i < columns_.size(); ++i) {
            widths[i] = std::max(widths[i], columns_[i].cached_header_width);
        }
        
        // Cell widths
        for (const auto& row : rows_) {
            for (size_t i = 0; i < std::min(row.size(), columns_.size()); ++i) {
                widths[i] = std::max(widths[i], row[i].cached_visible_width);
            }
        }
        
        // Apply min/max constraints
        for (size_t i = 0; i < columns_.size(); ++i) {
            if (columns_[i].min_width > 0) widths[i] = std::max(widths[i], columns_[i].min_width);
            if (columns_[i].max_width > 0) widths[i] = std::min(widths[i], columns_[i].max_width);
        }
        
        std::string out;
        out.reserve(8192);
        
        // Title
        if (!title_.empty()) {
            size_t total_width = std::accumulate(widths.begin(), widths.end(), 0) + 
                               (show_borders_ ? (widths.size() * 3 + 1) : (widths.size() - 1));
            Text title_text{title_};
            title_text.bold();
            Layout::append_justified(out, title_text.str(), title_text.visible_length(), total_width, Layout::Justify::Center);
            out.append("\n");
        }
        
        // Top border
        if (show_borders_) {
            out.append(to_string_view(border_style_[2])); // ┌
            for (size_t i = 0; i < widths.size(); ++i) {
                append_repeated(out, to_string_view(border_style_[0]), widths[i] + 2); // ─
                if (i < widths.size() - 1) out.append(to_string_view(border_style_[8])); // ┬
            }
            out.append(to_string_view(border_style_[3])).append("\n"); // ┐
        }
        
        // Header
        if (show_header_) {
            if (show_borders_) out.append(to_string_view(border_style_[1])); // │
            
            for (size_t i = 0; i < columns_.size(); ++i) {
                std::string header_text = truncate(columns_[i].header, widths[i]);
                Text styled_header{header_text};
                styled_header.bold();
                
                out.append(" ");
                Layout::append_justified(out, styled_header.str(), styled_header.visible_length(), widths[i], columns_[i].justify);
                out.append(" ");
                
                if (show_borders_) {
                    if (i < columns_.size() - 1) out.append(to_string_view(border_style_[1])); // │
                } else if (i < columns_.size() - 1) {
                    out.append(" ");
                }
            }
            
            out.append(show_borders_ ? to_string_view(border_style_[1]) : "").append("\n");
            
            // Header separator
            if (show_borders_) {
                out.append(to_string_view(border_style_[6])); // ├
                for (size_t i = 0; i < widths.size(); ++i) {
                    append_repeated(out, to_string_view(border_style_[0]), widths[i] + 2); // ─
                    if (i < widths.size() - 1) out.append(to_string_view(border_style_[10])); // ┼
                }
                out.append(to_string_view(border_style_[7])).append("\n"); // ┤
            }
        }
        
        // Rows
        for (const auto& row : rows_) {
            if (show_borders_) out.append(to_string_view(border_style_[1]));
            
            for (size_t i = 0; i < columns_.size(); ++i) {
                out.append(" ");
                if (i < row.size()) {
                    const auto& cell = row[i];
                    std::string raw_str = cell.content.str();
                    
                    if (cell.color || cell.style) {
                        std::string styled;
                        styled.reserve(raw_str.size() + 16);
                        if (cell.style) styled.append(*cell.style);
                        if (cell.color) styled.append(*cell.color);
                        styled.append(raw_str);
                        styled.append(style::reset);
                        
                        Layout::append_justified(out, styled, cell.cached_visible_width, widths[i], columns_[i].justify);
                    } else {
                        Layout::append_justified(out, raw_str, cell.cached_visible_width, widths[i], columns_[i].justify);
                    }
                } else {
                    out.append(widths[i], ' '); // Empty padded cell
                }
                out.append(" ");
                
                if (show_borders_) {
                    if (i < columns_.size() - 1) out.append(to_string_view(border_style_[1])); // │
                } else if (i < columns_.size() - 1) {
                    out.append(" ");
                }
            }
            
            if (show_borders_) out.append(to_string_view(border_style_[1])); // │
            out.append("\n");
        }
        
        // Bottom border
        if (show_borders_) {
            out.append(to_string_view(border_style_[4])); // └
            for (size_t i = 0; i < widths.size(); ++i) {
                append_repeated(out, to_string_view(border_style_[0]), widths[i] + 2); // ─
                if (i < widths.size() - 1) out.append(to_string_view(border_style_[9])); // ┴
            }
            out.append(to_string_view(border_style_[5])); // ┘
        }
        
        return out;
    }
    
    void display(std::ostream& os = std::cout) const {
        os << render() << std::endl;
    }
};

class Tree {
public:
    struct Node {
        Text label;
        std::vector<std::shared_ptr<Node>> children;
        bool expanded = true;
        
        Node(Text lbl) : label{std::move(lbl)} {}
        
        Node& add_child(Text child_label) {
            children.push_back(std::make_shared<Node>(std::move(child_label)));
            return *this;
        }
        
        Node& add_child(std::shared_ptr<Node> child) {
            children.push_back(std::move(child));
            return *this;
        }
    };
    
private:
    std::shared_ptr<Node> root_;
    std::vector<std::u8string_view> guide_style_ = {u8"│", u8" ", u8"├", u8"─", u8"└"}; // 0='│', 1=' ', 2='├', 3='─', 4='└'
    
    void render_node(std::ostream& os, 
                    const std::shared_ptr<Node>& node, 
                    const std::string& prefix = "",
                    bool is_last = true) const {
        // Current node
        os << prefix;
        if (!prefix.empty()) {
            os << (is_last ? to_string_view(guide_style_[4]) : to_string_view(guide_style_[2])) // └ or ├
               << to_string_view(guide_style_[3]) << to_string_view(guide_style_[3]) << " "; // ──
        }
        os << node->label << "\n";
        
        // Children
        if (node->expanded) {
            std::string child_prefix = prefix;
            if (!prefix.empty()) {
                child_prefix += (is_last ? "    " : repeat_view(1, guide_style_[0]) + "   "); // │
            } else {
                child_prefix += "    "; // For root node's children, just indentation
            }
            
            for (size_t i = 0; i < node->children.size(); ++i) {
                render_node(os, node->children[i], child_prefix, i == node->children.size() - 1);
            }
        }
    }
    
public:
    Tree(Text root_label) : root_{std::make_shared<Node>(std::move(root_label))} {}
    
    [[nodiscard]] std::shared_ptr<Node> root() const { return root_; }
    
    void display(std::ostream& os = std::cout) const {
        render_node(os, root_);
    }
};

class Panel {
    struct Padding {
        size_t top, right, bottom, left;
    };

    Text content_;
    std::optional<Text> title_;
    std::vector<std::u8string_view> border_style_ = {u8"─", u8"│", u8"┌", u8"┐", u8"└", u8"┘", u8"├", u8"┤"};
    std::optional<std::string_view> border_color_;
    std::optional<RGB> background_color_; // Not fully implemented in render yet for actual background color
    Padding padding_{1, 1, 1, 1};
    
public:
    Panel(Text content) : content_{std::move(content)} {}
    
    Panel& title(Text title) {
        title_ = std::move(title);
        return *this;
    }
    
    Panel& border_color(std::string_view color) {
        border_color_ = color;
        return *this;
    }
    
    Panel& background(RGB color) {
        background_color_ = color;
        return *this;
    }
    
    Panel& padding(size_t all) {
        padding_ = {all, all, all, all};
        return *this;
    }
    
    Panel& padding(size_t vertical, size_t horizontal) {
        padding_ = {vertical, horizontal, vertical, horizontal};
        return *this;
    }
    
    [[nodiscard]] std::string render(size_t width = 0) const {
        std::string content_str_with_styles = content_.str();
        std::vector<std::string> raw_content_lines; // Stores lines with original styles
        
        // Split content into lines
        std::stringstream ss{content_str_with_styles};
        std::string line;
        while (std::getline(ss, line)) {
            raw_content_lines.push_back(std::move(line));
        }
        
        // Calculate content width (visible characters only)
        size_t max_content_visible_width = 0;
        for (const auto& l : raw_content_lines) {
            max_content_visible_width = std::max(max_content_visible_width, Text{l}.visible_length());
        }
        
        size_t panel_width = max_content_visible_width + padding_.left + padding_.right + 2;
        if (width > 0) {
            panel_width = width; // Strictly obey requested width
        } else {
            // Auto-size, but cap at terminal width to prevent native wrapping
            size_t term_w = Terminal::width();
            if (term_w > 0 && panel_width > term_w) {
                panel_width = term_w;
            }
        }
        
        // Ensure we don't underflow if padding exceeds width
        size_t min_required_width = padding_.left + padding_.right + 2;
        if (panel_width < min_required_width) panel_width = min_required_width;

        size_t available_content_space = panel_width - padding_.left - padding_.right - 2;
        
        std::stringstream result_ss;
        
        // Helper to apply border color
        auto apply_border_color = [&](std::stringstream& s) {
            if (border_color_) s << *border_color_;
        };
        auto reset_color = [&](std::stringstream& s) {
            if (border_color_) s << style::reset;
        };

        // Top border
        apply_border_color(result_ss);
        result_ss << to_string_view(border_style_[2]); // ┌
        result_ss << repeat_view(panel_width - 2, border_style_[0]); // ─
        result_ss << to_string_view(border_style_[3]); // ┐
        reset_color(result_ss);
        result_ss << "\n";
        
        // Title
        if (title_) {
            std::string title_str = title_->str();
            size_t title_visible_len = title_->visible_length();
            size_t available_space = panel_width - 2; // excluding borders
            
            std::string display_title_content;
            size_t display_title_visible_len;
            if (title_visible_len > available_space) { // Title too long even with min padding
                // Calculate target visible length for the content itself (excluding "...")
                // For a title like "[  My Title  ]", the "..." replaces part of "My Title".
                // If available_space is 13, and "..." takes 3, then 10 chars are available for title text.
                size_t target_content_visible_len = (available_space > 3) ? (available_space - 3) : 0;
                display_title_content = truncate(title_str, target_content_visible_len) + "...";
                display_title_visible_len = count_visible_characters(display_title_content);
            } else {
                display_title_content = title_str;
                display_title_visible_len = title_visible_len;
            }

            size_t left_pad = 0;
            size_t right_pad = 0;
            
            // Prevent size_t underflow if the title is somehow wider than the available space
            if (available_space > display_title_visible_len) {
                left_pad = (available_space - display_title_visible_len) / 2;
                right_pad = available_space - display_title_visible_len - left_pad;
            } else if (available_space > 0) {
                // Extreme fallback: force title to fit exact available space
                display_title_content = truncate(display_title_content, available_space);
            }
            
            apply_border_color(result_ss);
            result_ss << to_string_view(border_style_[1]); // │
            reset_color(result_ss);
            
            result_ss << std::string(left_pad, ' ') 
                   << display_title_content
                   << std::string(right_pad, ' ');
                   
            apply_border_color(result_ss);
            result_ss << to_string_view(border_style_[1]); // │
            reset_color(result_ss);
            result_ss << "\n";
            
            // Title separator
            apply_border_color(result_ss);
            result_ss << to_string_view(border_style_[6]); // ├
            result_ss << repeat_view(panel_width - 2, border_style_[0]); // ─
            result_ss << to_string_view(border_style_[7]); // ┤
            reset_color(result_ss);
            result_ss << "\n";
        }
        
        // Content lines with padding
        for (size_t i = 0; i < padding_.top; ++i) {
            apply_border_color(result_ss);
            result_ss << to_string_view(border_style_[1]); // │
            reset_color(result_ss);
            result_ss << std::string(panel_width - 2, ' ');
            apply_border_color(result_ss);
            result_ss << to_string_view(border_style_[1]); // │
            reset_color(result_ss);
            result_ss << "\n";
        }
        
        for (const auto& raw_line : raw_content_lines) {
            apply_border_color(result_ss);
            result_ss << to_string_view(border_style_[1]); // │
            reset_color(result_ss);
            
            size_t line_visible_len = Text{raw_line}.visible_length();
            std::string display_line = raw_line;
            
            // Prevent line overflow which would break the right border
            if (line_visible_len > available_content_space) {
                display_line = truncate(raw_line, available_content_space) + std::string(style::reset);
                line_visible_len = available_content_space;
            }
            
            size_t filler_width = available_content_space - line_visible_len;

            result_ss << std::string(padding_.left, ' ')
                   << display_line
                   << std::string(filler_width, ' ')
                   << std::string(padding_.right, ' ');
            
            apply_border_color(result_ss);
            result_ss << to_string_view(border_style_[1]); // │
            reset_color(result_ss);
            result_ss << "\n";
        }
        
        for (size_t i = 0; i < padding_.bottom; ++i) {
            apply_border_color(result_ss);
            result_ss << to_string_view(border_style_[1]); // │
            reset_color(result_ss);
            result_ss << std::string(panel_width - 2, ' ');
            apply_border_color(result_ss);
            result_ss << to_string_view(border_style_[1]); // │
            reset_color(result_ss);
            result_ss << "\n";
        }
        
        // Bottom border
        apply_border_color(result_ss);
        result_ss << to_string_view(border_style_[4]); // └
        result_ss << repeat_view(panel_width - 2, border_style_[0]); // ─
        result_ss << to_string_view(border_style_[5]); // ┘
        reset_color(result_ss);
        
        return result_ss.str();
    }
    
    void display(std::ostream& os = std::cout, size_t width = 0) const {
        os << render(width) << std::endl;
    }
};

class LiveDisplay {
public:
    struct Config {
        std::chrono::milliseconds interval;
        bool clear_on_exit;
        bool start_on_new_line;
        bool use_alternate_screen = false;

        Config(
            std::chrono::milliseconds i = std::chrono::milliseconds{100},
            bool clear = false,
            bool new_line = true,
            bool alt_screen = false
        ) : interval(i), clear_on_exit(clear), start_on_new_line(new_line)
          , use_alternate_screen(alt_screen) 
        {}
    };

private:
    struct DisplaySlot {
        std::function<std::string()> renderer;
        bool active = true;
    };
    
    std::vector<DisplaySlot> slots_;
    std::jthread render_thread_;
    
    Config config_;
    std::mutex mutex_;

    // UI State caching for Diffing & Memory Reuse
    std::vector<std::string> front_buffer_;  // Currently on screen
    std::vector<std::string> back_buffer_;   // New frame being built
    std::string output_buffer_;              // Master ANSI command sequence
    size_t allocated_lines_{0};              // Physical terminal lines we've laid claim to
    
public:
    LiveDisplay(Config config = {})
        : config_(std::move(config))
    {
        // Pre-allocate decent capacity to prevent re-allocations
        front_buffer_.reserve(64);
        back_buffer_.reserve(64);
        output_buffer_.reserve(4096);
    }
    
    ~LiveDisplay() {
        stop();
    }
    
    size_t add_slot(std::function<std::string()> renderer) {
        std::lock_guard lock{mutex_};
        slots_.push_back({std::move(renderer), true});
        return slots_.size() - 1; // Return 0-indexed vector index
    }
    
    void update_slot(size_t index, std::function<std::string()> renderer) {
        std::lock_guard lock{mutex_};
        if (index < slots_.size()) {
            slots_[index].renderer = std::move(renderer);
        }
    }
    
    void remove_slot(size_t index) {
        std::lock_guard lock{mutex_};
        if (index < slots_.size()) {
            slots_[index].active = false;
        }
    }
    
    void start() {
        if (render_thread_.joinable()) return;
        
        if (config_.use_alternate_screen) {
            Terminal::alternate_screen();
        }

        Terminal::hide_cursor();
        if (config_.clear_on_exit) {
            std::cout << "\033[H\033[J"; // Clear screen, cursor to 1,1
        } else if (!config_.use_alternate_screen && config_.start_on_new_line) {
            std::cout << "\n"; // Ensure clean space for the first render
        }
        
        // Reset diffing state
        allocated_lines_ = 0;
        front_buffer_.clear();

        render_thread_ = std::jthread{[this](std::stop_token stoken) {
            while (!stoken.stop_requested()) {
                render_all();
                std::this_thread::sleep_for(config_.interval);
            }
            // Force one final render loop to guarantee 100% finished states are printed
            render_all();
        }};
    }
    
    void stop() {
        if (render_thread_.joinable()) {
            render_thread_.request_stop();
            render_thread_.join(); 
        }

        Terminal::show_cursor();

        // After thread stops, perform final cleanup
        if (config_.use_alternate_screen) {
            Terminal::main_screen();
            std::cout << std::flush;  // ensure everything is written
        } else if (config_.clear_on_exit) {
            std::cout << "\033[H\033[J"; // Clear entire screen
        } else {
            std::cout << "\n" << std::flush;
        }

    }

    // Public method to force a single render cycle, useful for final state updates
    void render_current_state() {
        render_all();
    }
    
private:
    void render_all() {
        size_t back_buffer_size = 0;

        // Lambda to push lines into back_buffer WITHOUT causing allocations
        // if the vector capacity already exists. C++17/20 string_view integration.
        auto push_line = [&](std::string_view sv) {
            if (back_buffer_size < back_buffer_.size()) {
                back_buffer_[back_buffer_size].assign(sv);
            } else {
                back_buffer_.emplace_back(sv);
            }
            back_buffer_size++;
        };

        {
            std::lock_guard lock{mutex_};
            for (const auto& slot : slots_) {
                if (!slot.active) continue;
                
                std::string content = slot.renderer();
                size_t start = 0, end;
                
                // Zero-allocation string splitting
                while ((end = content.find('\n', start)) != std::string::npos) {
                    push_line(std::string_view(content).substr(start, end - start));
                    start = end + 1;
                }
                // Push remainder
                if (start < content.size() || (content.empty() && back_buffer_size == 0)) {
                    push_line(std::string_view(content).substr(start));
                }
            }
        }

        output_buffer_.clear();
        bool alt_screen = config_.use_alternate_screen;

        // In relative mode, the terminal cursor sits just below our UI buffer (index: front_buffer_.size())
        int current_cursor_y = static_cast<int>(front_buffer_.size());
        size_t max_lines = std::max(front_buffer_.size(), back_buffer_size);
        
        // ---------------------------------------------------------------------
        // SCROLL SAFETY MECHANISM
        // If our UI needs more lines than we previously claimed, moving down
        // via `\033[B` won't work cleanly (it causes un-tracked scrolling).
        // We must manually print '\n' to guarantee the viewport pushes up.
        // ---------------------------------------------------------------------
        if (!alt_screen && back_buffer_size > allocated_lines_) {
            if (current_cursor_y < static_cast<int>(allocated_lines_)) {
                std::format_to(std::back_inserter(output_buffer_), "\033[{}B\r", allocated_lines_ - current_cursor_y);
                current_cursor_y = static_cast<int>(allocated_lines_);
            }
            for (size_t i = allocated_lines_; i < back_buffer_size; ++i) {
                output_buffer_ += "\n";
                current_cursor_y++;
            }
            allocated_lines_ = back_buffer_size;
        }

        // ---------------------------------------------------------------------
        // DIFFING AND CURSOR NAVIGATION
        // ---------------------------------------------------------------------
        for (size_t i = 0; i < max_lines; ++i) {
            bool old_exists = i < front_buffer_.size();
            bool new_exists = i < back_buffer_size;
            
            const std::string& old_line = old_exists ? front_buffer_[i] : "";
            const std::string& new_line = new_exists ? back_buffer_[i] : "";
            
            if (old_line != new_line) {
                if (alt_screen) {
                    // Absolute positioning is 1-indexed
                    std::format_to(std::back_inserter(output_buffer_), "\033[{};1H\033[2K{}", i + 1, new_line);
                } else {
                    // Calculate optimal relative cursor movement
                    int target_y = static_cast<int>(i);
                    if (current_cursor_y > target_y) {
                        std::format_to(std::back_inserter(output_buffer_), "\033[{}A\r", current_cursor_y - target_y);
                    } else if (current_cursor_y < target_y) {
                        std::format_to(std::back_inserter(output_buffer_), "\033[{}B\r", target_y - current_cursor_y);
                    } else {
                        output_buffer_ += "\r";
                    }
                    current_cursor_y = target_y;
                    
                    // Clear line & draw payload
                    std::format_to(std::back_inserter(output_buffer_), "\033[2K{}", new_line);
                }
            }
        }

        // ---------------------------------------------------------------------
        // RESTORE TERMINAL STATE
        // Move the cursor back below our active UI block so regular stdout
        // or user inputs aren't swallowed by our UI.
        // ---------------------------------------------------------------------
        if (!alt_screen) {
            int final_y = static_cast<int>(back_buffer_size);
            if (current_cursor_y < final_y) {
                std::format_to(std::back_inserter(output_buffer_), "\033[{}B\r", final_y - current_cursor_y);
            } else if (current_cursor_y > final_y) {
                std::format_to(std::back_inserter(output_buffer_), "\033[{}A\r", current_cursor_y - final_y);
            } else {
                output_buffer_ += "\r";
            }
            // Shrink physical allocation expectation if the UI contracted (e.g. bar disappeared)
            allocated_lines_ = back_buffer_size;
        }

        // ---------------------------------------------------------------------
        // BATCH EMIT
        // Send everything in one payload. `std::flush` guarantees execution.
        // ---------------------------------------------------------------------
        if (!output_buffer_.empty()) {
            std::cout << output_buffer_ << std::flush;
        }

        // Cache the newly rendered frame for the next diffing cycle
        front_buffer_.resize(back_buffer_size);
        for (size_t i = 0; i < back_buffer_size; ++i) {
            front_buffer_[i] = back_buffer_[i]; // Standard copy semantics
        }
    }
};

template<NumericLike T>
class ProgressBar {
public:
    struct Config {
        T total = 100;
        std::string description = "";
        bool show_percentage = true;
        bool show_value = true;
        bool show_speed = true;
        bool show_eta = true;
        bool show_elapsed = true;
        bool show_bar = true;
        bool show_counter = true;
        size_t width = 0; // 0 = auto
        std::u8string bar_style = u8"█";
        std::u8string empty_style = u8"░";
        std::string left_bracket = "[";
        std::string right_bracket = "]";
        std::string unit = "it";
        Gradient bar_gradient = {{RGB{255, 0, 0}, RGB{255, 255, 0}, RGB{0, 255, 0}}};
        bool expand = true; // Expand to terminal width
        size_t width_offset = 0; // Reserve space for container borders
    };
    
private:
    T* current_value_ptr_ = nullptr; // Pointer to the external value
    std::function<T()> current_value_getter_;
    T total_;
    Config config_;
    std::chrono::steady_clock::time_point start_time_;
    std::string custom_text_;
    std::mutex text_mutex_; // For custom text modification

    size_t bracket_visible_width_;
    size_t bar_char_visible_width_;
    size_t empty_char_visible_width_;

    std::vector<std::string> gradient_cache_;
    size_t last_bar_width_ = 0;

    void compute_width() {
        bracket_visible_width_ = count_visible_characters(config_.left_bracket + config_.right_bracket);
        bar_char_visible_width_ = std::max<size_t>(1, count_visible_characters(to_string_view(config_.bar_style)));
        empty_char_visible_width_ = std::max<size_t>(1, count_visible_characters(to_string_view(config_.empty_style)));
    }

    struct Speedometer {
        std::chrono::steady_clock::time_point last_time;
        T last_value{0};
        double current_speed{0.0};
        bool initialized{false};
        
        void update(T current_val) {
            auto now = std::chrono::steady_clock::now();

            if (!initialized) {
                last_time = now;
                last_value = current_val;
                initialized = true;
                return;
            }

            std::chrono::duration<double> time_diff = now - last_time;
            
            // Only update speed every ~500ms to keep the UI readable (stop jitter)
            if (time_diff.count() >= 0.5) {
                T value_diff = current_val - last_value;
                current_speed = value_diff / time_diff.count();
                last_time = now;
                last_value = current_val;
            }
        }

        [[nodiscard]] double speed() const { return current_speed; }
    
        [[nodiscard]] std::chrono::seconds eta(T remaining) const {
            return current_speed > 0 ? std::chrono::seconds(static_cast<long>(remaining / current_speed)) 
                                     : std::chrono::seconds{0};
        }
    };
    
    Speedometer speedometer_;
    
public:
    ProgressBar(T* value_ptr, Config config = {})
        : current_value_ptr_{value_ptr}
        , total_{config.total}
        , config_{std::move(config)}
        , start_time_{std::chrono::steady_clock::now()} 
    {
        compute_width();
    }

    ProgressBar(std::function<T()> value_getter, Config config = {})
        : current_value_getter_{std::move(value_getter)}
        , total_{config.total}
        , config_{std::move(config)}
        , start_time_{std::chrono::steady_clock::now()} 
    {
        compute_width();
    }
    
    // Call this externally when the tracked value changes
    void add_sample(T value) {
        speedometer_.update(value);
    }
    
    void set_custom_text(std::string text) {
        std::lock_guard lock{text_mutex_};
        custom_text_ = std::move(text);
    }
    
    [[nodiscard]] T value() const { return current_value_getter_ ? current_value_getter_() : *current_value_ptr_; }
    [[nodiscard]] T total() const { return total_; }

    [[nodiscard]] std::string render_string() {
        T current_ = current_value_getter_ ? current_value_getter_() : *current_value_ptr_; // Read current value
        speedometer_.update(current_);

        double percentage = total_ > 0 ? (static_cast<double>(current_) / total_) * 100.0 : 0.0;
        percentage = std::clamp(percentage, 0.0, 100.0);
        
        auto elapsed = std::chrono::steady_clock::now() - start_time_;
        auto eta = speedometer_.eta(total_ - current_);
        
        // === Build prefix ===
        std::string prefix;
        if (!config_.description.empty()) {
            prefix.append(Text{config_.description}.bold().str()).append(" ");
        }

        // === Build postfix (all text after the bar) ===
        std::string postfix;
        postfix.reserve(128);

        if (config_.show_percentage) {
            postfix.append(Text{std::format("{:6.2f}%", percentage)}.color(style::cyan).str()).append(" ");
        }
        if (config_.show_counter) {
            postfix.append(Text{std::format("{}/{}", current_, total_)}.color(style::green).str()).append(" ");
        }
        if (config_.show_speed) {
            double speed = speedometer_.speed();
            std::string unit;
            if (speed > 1'000'000) { speed /= 1'000'000; unit = "M"; }
            else if (speed > 1'000) { speed /= 1'000; unit = "K"; }
            unit += config_.unit;
            postfix.append(Text{std::format("({:.1f} {}/s)", speed, unit)}.color(style::yellow).str()).append(" ");
        }
        // [ELAPSED < ETA]
        if (config_.show_elapsed) {
            auto elapsed_sec = std::chrono::duration_cast<std::chrono::seconds>(elapsed).count();
            long h = elapsed_sec / 3600; elapsed_sec %= 3600;
            long m = elapsed_sec / 60; elapsed_sec %= 60;
            postfix.append("[")
                   .append(Text{std::format("{:02d}:{:02d}:{:02d}", h, m, elapsed_sec)}.color(style::blue).str())
                   .append(" < ");
        }
        if (config_.show_eta && total_ > 0) {
            auto eta_sec = eta.count();
            long h = eta_sec / 3600; eta_sec %= 3600;
            long m = eta_sec / 60; eta_sec %= 60;
            postfix.append(Text{std::format("{:02d}:{:02d}:{:02d}", h, m, eta_sec)}.color(style::magenta).str())
                   .append("] ");
        }

        {
            std::lock_guard lock{text_mutex_};
            if (!custom_text_.empty()) {
                postfix.append(Text{custom_text_}.color(style::bright_black).str()).append(" ");
            }
        }

        // === Calculate available width for the bar ===
        size_t term_width = Terminal::width();
        if (term_width > config_.width_offset) {
            term_width -= config_.width_offset; 
        }
        size_t prefix_len = count_visible_characters(prefix);
        size_t postfix_len = count_visible_characters(postfix);
        size_t extra_spaces = 1;  // space after right bracket
        
        if (term_width > postfix_len + bracket_visible_width_ + extra_spaces) {
            size_t max_prefix = term_width - postfix_len - bracket_visible_width_ - extra_spaces;
            if (prefix_len > max_prefix) {
                // Truncate the title and add '...' so stats stay visible
                prefix = truncate(prefix, max_prefix > 3 ? max_prefix - 3 : 0) + "... ";
                prefix_len = count_visible_characters(prefix);
            }
        } else {
            prefix.clear(); // Extreme case: terminal is too small for anything but stats
            prefix_len = 0;
        }

        size_t used = prefix_len + postfix_len + bracket_visible_width_ + extra_spaces;

        size_t bar_width = config_.width != 0 ? config_.width : 30;
        if (config_.expand) {
            size_t left = (term_width > used) ? (term_width - used) : 0;
            bar_width = (config_.width != 0) ? std::min(left, config_.width) : left;
        }

        if (bar_width != last_bar_width_) {
            gradient_cache_.clear();
            gradient_cache_.reserve(bar_width);
            for(size_t i = 0; i < bar_width; ++i) {
                gradient_cache_.push_back(config_.bar_gradient.at(static_cast<double>(i) / bar_width).to_ansi_foreground());
            }
            last_bar_width_ = bar_width;
        }

        std::string final_output;
        final_output.reserve(term_width + 256); // Pre-allocate enough for ANSI codes
        final_output.append(prefix);
        
        // Progress bar
        if (config_.show_bar) {
            final_output.append(config_.left_bracket);
            
            size_t filled_target = static_cast<size_t>(std::round(bar_width * percentage / 100.0));
            filled_target = std::clamp(filled_target, 0UL, bar_width);
            
            const std::string bar_style_str = to_string(config_.bar_style);
        
            for (size_t i = 0; i < filled_target; ++i) {
                if (i == 0 || gradient_cache_[i] != gradient_cache_[i-1]) {
                    final_output.append(gradient_cache_[i]);
                }
                final_output.append(bar_style_str);
            }
            
            if (filled_target > 0) final_output.append(style::reset);
            
            size_t empty_needed = bar_width - filled_target;
            if (empty_needed > 0) {
                std::string empty_str;
                empty_str.reserve(empty_needed * config_.empty_style.size());
                const std::string empty_style_str = to_string(config_.empty_style);
                for(size_t i = 0; i < empty_needed; ++i) empty_str.append(empty_style_str);
                
                final_output.append(style::bright_black).append(empty_str).append(style::reset);
            }
            final_output.append(config_.right_bracket).append(" ");
        }
        
        final_output.append(postfix);
        return final_output;
    }
};

class Spinner {
public:
    enum class Style {
        Dots,
        Line,
        Moon,
        Earth,
        Arrow,
        BouncingBar,
        Christmas,
        Hearts,
        Runner,
        Shark,
        Pong,
        Simple
    };
    
private:
    Style style_;
    std::vector<std::u8string> frames_;
    size_t current_frame_{0};
    std::string text_;
    // No internal thread, just a renderer
    
    void load_frames() {
        switch (style_) {
            case Style::Dots:
                frames_ = {u8".  ", u8".. ", u8"...", u8" ..", u8"  .", u8"   "};
                break;
            case Style::Line:
                frames_ = {u8"-", u8"\\", u8"|", u8"/"};
                break;
            case Style::Moon:
                frames_ = {u8"🌑", u8"🌒", u8"🌓", u8"🌔", u8"🌕", u8"🌖", u8"🌗", u8"🌘"};
                break;
            case Style::Arrow:
                frames_ = {u8"▹▹▹▹▹", u8"▸▹▹▹▹", u8"▹▸▹▹▹", u8"▹▹▸▹▹", u8"▹▹▹▸▹", u8"▹▹▹▹▸"};
                break;
            case Style::BouncingBar:
                frames_ = {u8"[    ]", u8"[=   ]", u8"[==  ]", u8"[=== ]", u8"[ ===]", u8"[  ==]", u8"[   =]", u8"[    ]", u8"[   =]", u8"[  ==]", u8"[ ===]", u8"[====]", u8"[=== ]", u8"[==  ]", u8"[=   ]"};
                break;
            case Style::Christmas:
                frames_ = {u8"🎄", u8"🎅", u8"🤶", u8"🦌", u8"🎁", u8"🔔", u8"❄️", u8"⛄"};
                break;
            case Style::Hearts:
                frames_ = {u8"💛", u8"💙", u8"💜", u8"💚", u8"❤️"};
                break;
            case Style::Runner:
                frames_ = {u8"🚶", u8"🏃"};
                break;
            case Style::Shark:
                frames_ = {u8"▐|\\____________▌", u8"▐_|\\___________▌", u8"▐__|\\__________▌", u8"▐___|\\_________▌", 
                          u8"▐____|\\________▌", u8"▐_____|\\_______▌", u8"▐______|\\______▌", u8"▐_______|\\_____▌",
                          u8"▐________|\\____▌", u8"▐_________|\\___▌", u8"▐__________|\\__▌", u8"▐___________|\\_▌",
                          u8"▐____________|\\▌", u8"▐____________/|▌", u8"▐___________/|_▌", u8"▐__________/|__▌",
                          u8"▐_________/|___▌", u8"▐________/|____▌", u8"▐_______/|_____▌", u8"▐______/|______▌",
                          u8"▐_____/|_______▌", u8"▐____/|________▌", u8"▐___/|_________▌", u8"▐__/|__________▌",
                          u8"▐_/|___________▌", u8"▐/|____________▌"};
                break;
            case Style::Simple:
                frames_ = {u8".", u8"..", u8"..."};
                break;
            default:
                frames_ = {u8"-", u8"\\", u8"|", u8"/"};
        }
    }
    
public:
    Spinner(Style style = Style::Line, std::string text = "")
        : style_{style}, text_{std::move(text)} {
        load_frames();
    }
    
    void set_text(std::string text) {
        text_ = std::move(text);
    }
    
    // Call this repeatedly to get the next frame string
    [[nodiscard]] std::string render_string() {
        std::string out;
        out.reserve(text_.size() + 32);

        if (!text_.empty()) {
            out.append(Text{text_}.color(style::cyan).str()).append(" ");
        }
        out.append(Text{to_string(frames_[current_frame_])}.color(style::yellow).str());
        
        current_frame_ = (current_frame_ + 1) % frames_.size();
        return out;
    }
};

class Status {
private:
    std::string message_;
    Spinner spinner_; // Use composition for spinner
    mutable std::mutex message_mutex_;
    std::chrono::steady_clock::time_point start_time_;
    bool show_time_{false};
    std::atomic<bool> success_ = false; // To indicate final state
    std::atomic<bool> stopped_ = false; // To indicate it's done animating
    
public:
    Status(std::string message = "", bool show_time = false)
        : message_{std::move(message)}
        , spinner_{Spinner::Style::Line, ""} // Initialize spinner, text handled in render
        , start_time_{std::chrono::steady_clock::now()}
        , show_time_{show_time}
    {}
    
    // Call this repeatedly to get the status string
    [[nodiscard]] std::string render_string() {
        std::string out;
        out.reserve(message_.size() + 64);
        
        std::unique_lock<std::mutex> lock(message_mutex_); 

        if (!stopped_.load(std::memory_order_acquire)) {
            out.append(spinner_.render_string()); 
        } else {
            out.append(success_.load(std::memory_order_acquire) ? 
                       Text{"✓"}.color(style::green).str() : 
                       Text{"✗"}.color(style::red).str());
        }

        out.append(" ")
           .append(Text{message_}.color(style::bright_black).str())
           .append(" ");
        
        lock.unlock();

        // Time if enabled
        if (show_time_) {
            auto elapsed = std::chrono::steady_clock::now() - start_time_;
            auto seconds = std::chrono::duration_cast<std::chrono::seconds>(elapsed);
            out.append(Text{std::format("({}s)", seconds.count())}.color(style::dim).str());
        }
        
        return out;
    }
    
    void update_message(std::string message) {
        std::lock_guard<std::mutex> lock(message_mutex_);
        message_ = std::move(message);
    }

    void succeed() { 
        success_.store(true, std::memory_order_release);
        stopped_.store(true, std::memory_order_release);
    }
    void fail() {
        success_.store(false, std::memory_order_release);
        stopped_.store(true, std::memory_order_release);
    }
    void stop_animation_early() { // Use if you just want to stop, not necessarily succeed/fail
        stopped_.store(true, std::memory_order_release);
    }
};

class Rule {
    std::string title_;
    char character_ = '-';
    std::optional<std::string_view> color_;
    Layout::Justify align_{Layout::Justify::Center};
    
public:
    Rule(std::string title = "", char character = '-')
        : title_{std::move(title)}, character_{character} {}
    
    Rule& color(std::string_view color) {
        color_ = color;
        return *this;
    }
    
    Rule& align(Layout::Justify alignment) {
        align_ = alignment;
        return *this;
    }
    
    [[nodiscard]] std::string render(size_t width = 0) const {
        if (width == 0) width = Terminal::width();
        if (width < 2) return ""; // Minimum width for a rule
        
        std::stringstream ss;
        
        if (title_.empty()) {
            std::string line(width, character_);
            if (color_) {
                ss << Text{line}.color(*color_);
            } else {
                ss << line;
            }
            return ss.str();
        }
        
        size_t title_len = Text{title_}.visible_length();
        size_t total_dashes = width > title_len + 2 ? width - title_len - 2 : 0; // 2 for spaces
        
        if (total_dashes < 2) {
            // Not enough space, just show title (possibly truncated)
            std::string truncated_title = title_;
            if (title_len > width) {
                size_t actual_trunc_len = width > 3 ? title_len - (title_len - (width - 3)) : 0;  // = width - 3
                truncated_title = truncate(title_.substr(0, actual_trunc_len), actual_trunc_len);
                truncated_title += "...";
            }
            if (color_) {
                ss << Text{truncated_title}.color(*color_).bold();
            } else {
                ss << Text{truncated_title}.bold();
            }
            return ss.str();
        }
        
        size_t left_dashes, right_dashes;
        
        switch (align_) {
            case Layout::Justify::Left:
                left_dashes = 0; // No dashes on left, just title and then dashes
                right_dashes = total_dashes;
                break;
            case Layout::Justify::Right:
                left_dashes = total_dashes;
                right_dashes = 0; // No dashes on right
                break;
            case Layout::Justify::Center:
            default:
                left_dashes = total_dashes / 2;
                right_dashes = total_dashes - left_dashes;
                break;
        }
        
        std::string left_line(left_dashes, character_);
        std::string right_line(right_dashes, character_);
        
        if (color_) {
            ss << Text{left_line}.color(*color_);
            if (!left_line.empty()) ss << " ";
        } else {
            ss << left_line;
            if (!left_line.empty()) ss << " ";
        }
        ss << Text{title_}.bold();  // Always bold
        if (!right_line.empty()) ss << " ";
        if (color_) {
            ss << Text{right_line}.color(*color_);
        } else {
            ss << right_line;
        }
        
        return ss.str();
    }
    
    void display(std::ostream& os = std::cout, size_t width = 0) const {
        os << render(width) << std::endl;
    }
};

class ProgressLogger {
private:
    struct LogEntry {
        std::chrono::system_clock::time_point time;
        std::string level;
        Text message;
    };
    
    std::deque<LogEntry> entries_; // Use deque for efficient pop_front
    size_t max_entries_{1000};
    bool show_timestamps_{true};
    bool show_levels_{true};
    std::map<std::string, std::string_view> level_colors_ = {
        {"DEBUG", style::dim},
        {"INFO", style::green},
        {"WARNING", style::yellow},
        {"ERROR", style::red},
        {"CRITICAL", style::bright_red}
    };
    mutable std::mutex entries_mutex_; // Protect access to entries_
    
public:
    ProgressLogger& set_max_entries(size_t max) {
        std::lock_guard lock(entries_mutex_);
        max_entries_ = max;
        while (entries_.size() > max_entries_) {
            entries_.pop_front();
        }
        return *this;
    }

    void log(std::string level, std::string message) {
        auto now = std::chrono::system_clock::now();
        Text formatted_msg{std::move(message)};

        std::lock_guard lock(entries_mutex_);
        entries_.push_back({now, std::move(level), std::move(formatted_msg)});
        
        if (entries_.size() > max_entries_) {
            entries_.pop_front();
        }
    }
    
    void debug(std::string message) { log("DEBUG", std::move(message)); }
    void info(std::string message) { log("INFO", std::move(message)); }
    void warning(std::string message) { log("WARNING", std::move(message)); }
    void error(std::string message) { log("ERROR", std::move(message)); }
    void critical(std::string message) { log("CRITICAL", std::move(message)); }
    
    [[nodiscard]] std::string get_recent(size_t count = 10) const {
        std::vector<LogEntry> snapshot;

        {
            std::lock_guard lock(entries_mutex_);
            count = std::min(count, entries_.size());
            size_t start_idx = entries_.size() - count;
            snapshot.assign(entries_.begin() + start_idx, entries_.end());
        }
        
        std::string out;
        out.reserve(count * 128);

        for (const auto& entry : snapshot) {
            if (show_timestamps_) {
                auto time_t = std::chrono::system_clock::to_time_t(entry.time);
                std::tm tm;
#ifdef _WIN32
                localtime_s(&tm, &time_t);
#else
                localtime_r(&time_t, &tm);
#endif
                out.append(std::format("{:02}:{:02}:{:02} ", tm.tm_hour, tm.tm_min, tm.tm_sec));
            }
            
            if (show_levels_) {
                auto color_it = level_colors_.find(entry.level);
                if (color_it != level_colors_.end()) {
                    out.append(Text{entry.level}.color(color_it->second).str()).append(" ");
                } else {
                    out.append(entry.level).append(" ");
                }
            }
            
            out.append(entry.message.str()).append("\n");
        }
        return out;
    }

    [[nodiscard]] std::string render_recent(size_t count = 10) const {
        std::lock_guard lock(entries_mutex_); // Protect access to entries_
        
        count = std::min(count, entries_.size()); // Don't try to get more entries than available
        
        std::stringstream ss;
        // Iterate from the Nth last entry to the most recent
        size_t start_idx = entries_.size() - count;
        for (size_t i = start_idx; i < entries_.size(); ++i) {
            const auto& entry = entries_[i];
            
            if (show_timestamps_) {
                auto time_t = std::chrono::system_clock::to_time_t(entry.time);
                std::tm tm;
#ifdef _WIN32
                localtime_s(&tm, &time_t);
#else
                localtime_r(&time_t, &tm);
#endif
                ss << std::put_time(&tm, "%H:%M:%S") << " ";
            }
            
            if (show_levels_) {
                auto color_it = level_colors_.find(entry.level);
                if (color_it != level_colors_.end()) {
                    ss << Text{entry.level}.color(color_it->second) << " ";
                } else {
                    ss << entry.level << " ";
                }
            }
            
            ss << entry.message << "\n"; // Each entry on a new line
        }
        return ss.str();
    }
    
    void display_recent(std::ostream& os = std::cout, size_t count = 10) const {
        auto recent = get_recent(count);
        for (const auto& entry : recent) {
            os << entry << "\n";
        }
        os << std::flush;
    }
};

// ConsoleCapture can be used to temporarily redirect std::cout/cerr
class ConsoleCapture {
private:
    std::streambuf* original_cout_;
    std::streambuf* original_cerr_;
    std::stringstream buffer_;
    
public:
    ConsoleCapture() {
        original_cout_ = std::cout.rdbuf();
        original_cerr_ = std::cerr.rdbuf();
        std::cout.rdbuf(buffer_.rdbuf());
        std::cerr.rdbuf(buffer_.rdbuf());
    }
    
    ~ConsoleCapture() {
        release();
    }
    
    void release() {
        if (original_cout_) {
            std::cout.rdbuf(original_cout_);
            original_cout_ = nullptr;
        }
        if (original_cerr_) {
            std::cerr.rdbuf(original_cerr_);
            original_cerr_ = nullptr;
        }
    }
    
    [[nodiscard]] std::string get_output() const {
        return buffer_.str();
    }

    std::stringstream& get_buffer() {
        return buffer_;
    }
    
    void clear() {
        buffer_.str("");
        buffer_.clear();
    }
};

} // namespace progressbar
