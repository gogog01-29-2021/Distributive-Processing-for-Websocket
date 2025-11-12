# bigdata

---
## 환경 설정
* x64 + /std:c++17.
* 빌드 전제: boost-asio, boost-beast, openssl, nlohmann-json 설치(vcpkg 권장).
++추가
.\vcpkg.exe install boost-lockfree:x64-windows

```
vcpkg install boost-asio:x64-windows boost-beast:x64-windows openssl:x64-windows nlohmann-json:x64-windows
vcpkg integrate install
```
## 실행(단일 인스턴스)
 .\orderbook_rt.exe --symbols BTCUSDT,ETHUSDT,SOLUSDT --instance A --seconds 60