#pragma once

// MSE — BitTorrent Protocol Encryption (Message Stream Encryption), wire
// format v1.0 as implemented by libtorrent/Transmission/qBittorrent.
//
// Byte-level format (verified against real peers; prime and key derivation
// taken verbatim from libtorrent's pe_crypto.cpp):
//
//   A -> B : [Ya 96][PadA 0-511]
//   B -> A : [Yb 96][PadB 0-511]
//   A -> B : [HASH('req1',S) 20][HASH('req2',SKEY) XOR HASH('req3',S) 20]
//            [RC4( VC 8 | crypto_provide 4 | len(PadC) 2 | PadC | len(IA) 2 )]
//            [RC4( IA )]   <- the 68-byte BT handshake, always RC4 here
//   B -> A : [RC4( VC 8 | crypto_select 4 | len(PadD) 2 | PadD )]
//            [RC4( IB )]   <- B's 68-byte BT handshake (plaintext if selected)
//
//   S = 96-byte DH shared secret (right-aligned, zero-padded)
//   KEY_A = SHA1("keyA" || S || SKEY)  -> encrypts A->B
//   KEY_B = SHA1("keyB" || S || SKEY)  -> encrypts B->A
//   crypto bits: 0x01 = plaintext, 0x02 = RC4
//   RC4 streams discard the first 1024 keystream bytes, then run continuously.
//
// This header holds the pure-crypto pieces; the async negotiation state
// machine lives in AsyncSocket (MseHandshake).

#include <openssl/bn.h>
#include <openssl/rand.h>
#include <openssl/sha.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>

#include "Utils.hpp"

namespace mse {

constexpr size_t kDhKeySize = 96;      // DH pubkeys and shared secret (768 bit)
constexpr size_t kRc4DiscardBytes = 1024;
constexpr size_t kMaxPadSize = 512;
constexpr size_t kHandshakeLen = 68;   // BT handshake size (19+8+20+20+1)

constexpr uint32_t kCryptoPlaintext = 0x00000001;
constexpr uint32_t kCryptoRc4 = 0x00000002;
constexpr uint32_t kCryptoAll = kCryptoPlaintext | kCryptoRc4;

using Secret = std::array<std::byte, kDhKeySize>;

// ---- RC4 (ARC4) ----
class Rc4 {
public:
    Rc4() = default;

    void init(std::span<const std::byte> key) {
        if (key.empty()) {
            throw std::runtime_error("MSE: RC4 key must not be empty");
        }
        s_.fill(0);
        for (int i = 0; i < 256; ++i) {
            s_[i] = static_cast<uint8_t>(i);
        }
        uint8_t j = 0;
        for (int i = 0; i < 256; ++i) {
            j = static_cast<uint8_t>(j + s_[i] + static_cast<uint8_t>(key[i % key.size()]));
            std::swap(s_[i], s_[j]);
        }
        i_ = 0;
        j_ = 0;
    }

    // In-place encrypt/decrypt (RC4 is symmetric).
    void crypt(std::span<std::byte> data) {
        for (auto& b : data) {
            i_ = static_cast<uint8_t>(i_ + 1);
            j_ = static_cast<uint8_t>(j_ + s_[i_]);
            std::swap(s_[i_], s_[j_]);
            const uint8_t k = s_[static_cast<uint8_t>(s_[i_] + s_[j_])];
            b = static_cast<std::byte>(static_cast<uint8_t>(b) ^ k);
        }
    }

private:
    std::array<uint8_t, 256> s_{};
    uint8_t i_ = 0;
    uint8_t j_ = 0;
};

// ---- 768-bit Diffie-Hellman ----
// Prime from libtorrent pe_crypto.cpp (identical in libtorrent 1.2/2.0 and
// the Vuze v1.0 spec). NOTE: this is RFC 2409 group 1 with a modified tail
// (...0000000000090563) — using the stock RFC 2409 constant breaks the
// handshake against every real client.
class Dh {
public:
    static constexpr std::string_view kPrimeHex =
        "FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD1"
        "29024E088A67CC74020BBEA63B139B22514A08798E3404DD"
        "EF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245"
        "E485B576625E7EC6F44C42E9A63A36210000000000090563";

    Dh() {
        prime_ = BN_new();
        // BN_hex2bn returns the number of hex digits consumed (192), or 0 on
        // error — check for 0, not 1.
        if (!prime_ || BN_hex2bn(&prime_, std::string(kPrimeHex).c_str()) == 0) {
            throw std::runtime_error("MSE: failed to load DH prime");
        }
        ctx_ = BN_CTX_new();
        if (!ctx_) {
            throw std::runtime_error("MSE: BN_CTX_new failed");
        }
    }

    ~Dh() {
        if (ctx_) {
            BN_CTX_free(ctx_);
        }
        if (prime_) {
            BN_free(prime_);
        }
        if (priv_) {
            BN_free(priv_);
        }
        if (pub_) {
            BN_free(pub_);
        }
    }

    Dh(const Dh&) = delete;
    Dh& operator=(const Dh&) = delete;

    // 96 random bytes as the private exponent; public key = 2^priv mod P.
    Secret generate_keypair() {
        priv_ = BN_new();
        pub_ = BN_new();
        if (!priv_ || !pub_) {
            throw std::runtime_error("MSE: BN_new failed");
        }
        std::array<uint8_t, kDhKeySize> priv_bytes{};
        if (RAND_bytes(priv_bytes.data(), static_cast<int>(priv_bytes.size())) != 1) {
            throw std::runtime_error("MSE: RAND_bytes failed");
        }
        BN_bin2bn(priv_bytes.data(), static_cast<int>(priv_bytes.size()), priv_);
        BIGNUM* two = BN_new();
        if (!two || BN_set_word(two, 2) != 1) {
            if (two) {
                BN_free(two);
            }
            throw std::runtime_error("MSE: BN_set_word failed");
        }
        if (BN_mod_exp(pub_, two, priv_, prime_, ctx_) != 1) {
            BN_free(two);
            throw std::runtime_error("MSE: BN_mod_exp failed");
        }
        BN_free(two);
        return pub_bytes();
    }

    // S = peer_pub^priv mod P, 96-byte big-endian (right-aligned, zero-padded).
    Secret compute_secret(const Secret& peer_pub) {
        if (!priv_) {
            throw std::runtime_error("MSE: generate_keypair must be called first");
        }
        BIGNUM* peer = BN_bin2bn(reinterpret_cast<const unsigned char*>(peer_pub.data()),
                                 static_cast<int>(peer_pub.size()), nullptr);
        if (!peer) {
            throw std::runtime_error("MSE: BN_bin2bn(peer pub) failed");
        }
        BIGNUM* secret = BN_new();
        if (!secret || BN_mod_exp(secret, peer, priv_, prime_, ctx_) != 1) {
            BN_free(peer);
            if (secret) {
                BN_free(secret);
            }
            throw std::runtime_error("MSE: BN_mod_exp(secret) failed");
        }
        BN_free(peer);

        Secret out{};
        if (BN_bn2binpad(secret, reinterpret_cast<unsigned char*>(out.data()),
                         static_cast<int>(out.size())) < 0) {
            BN_free(secret);
            throw std::runtime_error("MSE: BN_bn2binpad failed");
        }
        BN_free(secret);
        return out;
    }

    Secret pub_bytes() const {
        Secret out{};
        if (!pub_) {
            throw std::runtime_error("MSE: generate_keypair must be called first");
        }
        if (BN_bn2binpad(pub_, reinterpret_cast<unsigned char*>(out.data()),
                         static_cast<int>(out.size())) < 0) {
            throw std::runtime_error("MSE: BN_bn2binpad(pub) failed");
        }
        return out;
    }

private:
    BIGNUM* prime_ = nullptr;
    BIGNUM* priv_ = nullptr;
    BIGNUM* pub_ = nullptr;
    BN_CTX* ctx_ = nullptr;
};

// ---- Key derivation helpers ----
// Concatenate the inputs and one-shot SHA1 them (the low-level SHA1_Init/
// Update/Final API is deprecated in OpenSSL 3; the one-shot SHA1() used by
// Crypto::calculate_sha1_hash_data is not).
inline std::array<std::byte, 20> sha1_concat(std::span<const std::byte> a,
                                             std::span<const std::byte> b) {
    std::vector<std::byte> buf;
    buf.reserve(a.size() + b.size());
    buf.insert(buf.end(), a.begin(), a.end());
    buf.insert(buf.end(), b.begin(), b.end());
    auto hash = Crypto::calculate_sha1_hash_data(buf);
    std::array<std::byte, 20> out{};
    std::ranges::copy(hash, out.begin());
    return out;
}

inline std::array<std::byte, 20> sha1_concat3(std::span<const std::byte> a,
                                              std::span<const std::byte> b,
                                              std::span<const std::byte> c) {
    std::vector<std::byte> buf;
    buf.reserve(a.size() + b.size() + c.size());
    buf.insert(buf.end(), a.begin(), a.end());
    buf.insert(buf.end(), b.begin(), b.end());
    buf.insert(buf.end(), c.begin(), c.end());
    auto hash = Crypto::calculate_sha1_hash_data(buf);
    std::array<std::byte, 20> out{};
    std::ranges::copy(hash, out.begin());
    return out;
}

inline std::array<std::byte, 4> to_be32(uint32_t v) {
    std::array<std::byte, 4> out{};
    out[0] = static_cast<std::byte>((v >> 24) & 0xFF);
    out[1] = static_cast<std::byte>((v >> 16) & 0xFF);
    out[2] = static_cast<std::byte>((v >> 8) & 0xFF);
    out[3] = static_cast<std::byte>(v & 0xFF);
    return out;
}

inline uint32_t from_be32(std::span<const std::byte> b) {
    return (static_cast<uint32_t>(b[0]) << 24) | (static_cast<uint32_t>(b[1]) << 16)
         | (static_cast<uint32_t>(b[2]) << 8) | static_cast<uint32_t>(b[3]);
}

inline std::array<std::byte, 2> to_be16(uint16_t v) {
    std::array<std::byte, 2> out{};
    out[0] = static_cast<std::byte>((v >> 8) & 0xFF);
    out[1] = static_cast<std::byte>(v & 0xFF);
    return out;
}

inline uint16_t from_be16(std::span<const std::byte> b) {
    return static_cast<uint16_t>((static_cast<uint16_t>(b[0]) << 8) | static_cast<uint16_t>(b[1]));
}

// ---- Per-connection negotiated state ----
// `encrypt`/`decrypt` hold the final streams (1024-byte discard applied at
// init). Once `active` is set, AsyncSocket encrypts every send and decrypts
// every receive through these instances, continuing the stream.
struct MseCrypto {
    bool active = false;   // negotiation done; all subsequent I/O goes through streams
    bool rc4 = false;      // negotiated method (false = plaintext after handshake)
    Rc4 encrypt;
    Rc4 decrypt;
};

} // namespace mse
