# Copilot Instructions for DayTradingBot

요약: 이 파일은 AI 코딩 에이전트(예: Copilot)가 이 저장소에서 즉시 생산적으로 작업할 수 있도록 핵심 구조, 주요 파일/경로, 개발 워크플로우와 예시를 간결하게 안내합니다.

**빅픽처(아키텍처 요약)**
- `API/` : 브로커별 드라이버와 인증/로그인 로직. 주요 서브폴더: `Daishin/`, `Kiwoom/`.
  - `API/Daishin/api.py`, `API/Daishin/login.py`, `API/Daishin/auth.py` — 대신증권 연동 관련 구현.
  - `API/Kiwoom/api.py`, `API/Kiwoom/login.py`, `API/Kiwoom/rest_api.py`, `API/Kiwoom/ticker_handler.py` — 키움 연동 및 REST/티커 처리.
- `Collector/` : 실시간/분단위 데이터 수집기. 핵심 파일: `Collector/minute_collector.py`.
- `BackTest/` : 백테스트 구현체. `BackTest/backtest.py`, `BackTest/volatility_backtest.py`가 전략별 백테스트 루틴을 포함.
- `Strategy/` : 전략 구현체. 예: `Strategy/volatility_breakout.py`, `Strategy/strategy.py`(공통 유틸).
- `data/` : 수집된 차트/백테스트 데이터와 티커 목록. 백테스트 CSV는 `data/backtest/VolatilityBreakout/`에 위치.
- 엔트리포인트: `main_test.py` — 개발 중 빠른 실행/검증에 사용되는 스크립트(프로젝트 루트).

**핵심 통합 포인트와 데이터 흐름**
- 수집: `Collector/minute_collector.py`가 거래소/브로커 API를 통해 분 단위 데이터를 생성 → `data/chart/minute_pkl/` 또는 `data/ticker/`에 저장.
- 전략 실행: `Strategy/*`의 전략이 `API/*`의 브로커 드라이버를 호출하여 주문/조회 수행.
- 백테스트: `BackTest/*`는 `data/backtest/...`의 CSV/피클을 읽어 전략을 시뮬레이션.

**프로젝트 특정 규칙 / 관찰된 컨벤션**
- 브로커별 로직은 `API/<BrokerName>/` 하위에 모아둠 — 새로운 브로커를 추가할 때는 동일한 구조(`api.py`, `login.py`, 인증 관련 파일`)를 따르세요.
- 전략은 `Strategy/`에 파일 단위로 추가. 백테스트는 `BackTest/`의 백테스트 스크립트를 재사용하도록 설계되어 있음.
- 민감한 정보(계정, 인증)는 각 `API/*/login.py` 또는 `auth.py`에서 처리됩니다. 실제 크리덴셜은 저장소에 커밋하면 안 됩니다.

**개발 워크플로우(발견 가능한 단축 명령)**
- 빠른 실행(개발용):

  ```bash
  python main_test.py
  ```

- 개별 스크립트 실행(예시):

  ```bash
  python BackTest/backtest.py
  python BackTest/volatility_backtest.py
  python Collector/minute_collector.py
  ```

(참고: 환경/의존성 정보가 명시된 파일은 없으므로 Python 3.8+ 가정. 가상환경과 필요한 패키지 목록을 추가하면 좋습니다.)

**수정 시 유의점 / PR 가이드**
- 브로커 연동 코드를 변경할 때는 해당 브로커 폴더의 `login.py`/`api.py`를 함께 검토하세요.
- 데이터 포맷(백테스트 CSV, 피클 위치)을 변경하면 `BackTest/`과 `Collector/`를 동시에 업데이트해야 합니다.
- 새 전략을 추가하면 `Strategy/`에 파일 추가 후 `main_test.py` 또는 해당 백테스트 스크립트에서 호출 지점을 추가하세요.

**구체적 예시(참조 코드 위치)**
- 주문/조회 연동 확인: [API/Daishin/api.py](API/Daishin/api.py)
- 티커 이벤트 처리: [API/Kiwoom/ticker_handler.py](API/Kiwoom/ticker_handler.py)
- 전략 구현 예시: [Strategy/volatility_breakout.py](Strategy/volatility_breakout.py)
- 백테스트 엔진: [BackTest/backtest.py](BackTest/backtest.py)

피드백 요청: 이 초안에서 더 추가하길 원하는 구체적 정보(예: 요구되는 Python 패키지 목록, CI/테스트 명령, 혹은 권한/환경 설정 절차)가 있으면 알려 주세요. 해당 정보를 반영해 파일을 빠르게 업데이트하겠습니다.
