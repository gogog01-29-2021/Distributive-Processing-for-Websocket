## 실행 환경 안내 (README.md)

본 문서는 Binance / Bybit / OKX 멀티거래소 실시간 오더북 시스템을 Windows 환경에서 실행하기 위한 빌드 및 실행 환경 요구사항을 정리한 것입니다.

## 1. 개발 및 실행 환경
| 항목 | 설정 |
|------|------|
| **OS** | Windows 10 / 11 (64-bit) |
| **Compiler** | MSVC (cl.exe, Visual Studio Build Tools) |
| **Package Manager** | vcpkg (`x64-windows`) |
| **C++ Standard** | C++17 |
| **CRT** | `/MD` (Dynamic CRT) |
| **Dependencies** | OpenSSL, Boost.Beast, Boost.Asio, Boost.Lockfree, AWS SDK (C++), nlohmann-json |

---

## 2. 필수 의존 라이브러리 (vcpkg)

다음 패키지들이 vcpkg에서 설치되어 있어야 한다.

```
vcpkg install openssl:x64-windows
vcpkg install boost-beast:x64-windows
vcpkg install boost-asio:x64-windows
vcpkg install boost-lockfree:x64-windows
vcpkg install aws-sdk-cpp[kinesis]:x64-windows
vcpkg install nlohmann-json:x64-windows
```

설치 폴더 예시:

```
C:\BIGDATA3\bigdata\vcpkg\installed\x64-windows\
```

## 3. 빌드 설정 (VS Code tasks.json)

실제 사용 중인 빌드 설정:
```
{
  "version": "2.0.0",
  "tasks": [
    {
      "label": "C/C++: cl.exe build active file",
      "type": "shell",
      "command": "cl.exe",
      "args": [
        "/nologo",
        "/Zi",
        "/EHsc",
        "/std:c++17",
        "/MD",                                   // 🔹 Dynamic CRT
        "/DWIN32_LEAN_AND_MEAN",
        "/D_WIN32_WINNT=0x0A00",
        "/DBOOST_ERROR_CODE_HEADER_ONLY",

        "/I", "C:/BIGDATA3/bigdata/vcpkg/installed/x64-windows/include",
        "main.cpp",
        "/FoC:/BIGDATA3/bigdata/build/",
        "/FeC:/BIGDATA3/bigdata/orderbook_rt.exe",

        "/link",
        "/LIBPATH:C:/BIGDATA3/bigdata/vcpkg/installed/x64-windows/lib",

        "/NODEFAULTLIB:libcpmt",                 // 🔹 정적 CRT 제거

        "libssl.lib",
        "libcrypto.lib",
        "Ws2_32.lib",
        "Crypt32.lib",
        "aws-cpp-sdk-kinesis.lib",
        "aws-cpp-sdk-core.lib"
      ],
      "options": {
        "cwd": "C:/BIGDATA3/bigdata"
      },
      "group": {
        "kind": "build",
        "isDefault": true
      },
      "problemMatcher": [
        "$msCompile"
      ]
    }
  ]
}
```

## 4. 실행 시 필요한 DLL 목록

vcpkg(동적 CRT) 기반이므로 exe 실행 시 다음 DLL들이 필요하다.

```
AWS SDK DLL

aws-cpp-sdk-core.dll

aws-cpp-sdk-kinesis.dll

AWS Common Runtime(CRT) DLL

aws-c-common.dll

aws-c-io.dll

aws-c-cal.dll

aws-c-compression.dll

aws-c-http.dll

aws-c-event-stream.dll

aws-checksums.dll

OpenSSL DLL

libssl-3-x64.dll

libcrypto-3-x64.dll
```

Boost / 기타 DLL이 필요한 경우 자동 포함
## 5. DLL 로딩 방식 (중요)

실행 파일이 정상 실행되기 위해서는 다음 중 한 가지 방식이 필수이다.

✔️ 옵션 A: PATH 환경변수에 추가 (권장)

PowerShell에서:

```
$env:PATH += ";C:\BIGDATA3\bigdata\vcpkg\installed\x64-windows\bin"
.\orderbook_rt.exe

```

✔️ 옵션 B: DLL을 exe 옆에 복사

```
cd C:\BIGDATA3\bigdata

copy C:\BIGDATA3\bigdata\vcpkg\installed\x64-windows\bin\aws-*.dll .
copy C:\BIGDATA3\bigdata\vcpkg\installed\x64-windows\bin\libssl-*.dll .
copy C:\BIGDATA3\bigdata\vcpkg\installed\x64-windows\bin\libcrypto-*.dll .
```

## 6. 실행 방법
```
cd C:\BIGDATA3\bigdata
.\orderbook_rt.exe
```

실행되면 다음과 같은 로그가 출력된다:
```
Running (Binance + OKX + Bybit / Ingestor A/B / Validator per symbol)...
[ingestor:binance:A] ...
[validator:BTCUSDT] ...
```
