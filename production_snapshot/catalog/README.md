# D* 생산 당시 코드 보존 — 2026-11-08

생산 당시 소스, 보관된 실제 제출 PSet, 사용한 ONNX 모델을 함께 보존한다.
현재 작업 중인 매칭 수정본과 나중에 추가된 resolution 모듈은 포함하지 않았다.

| 생산본 | 보존 tag | 적용 범위 |
|---|---|---|
| AN DATA 0311 | [production/an-data-0311](https://github.com/JunseokLee3609/VertexCompositeAnalysis/tree/production/an-data-0311) | AN에 사용한 March DATA 생산 |
| AN MC 01Sep26 | [production/an-mc-01sep26-pat6siggen-v1](https://github.com/JunseokLee3609/VertexCompositeAnalysis/tree/production/an-mc-01sep26-pat6siggen-v1) | 공식 prompt/nonprompt 6개 샘플 |
| D* vertex-refit Before/After | [production/dstar-refit-diag-30sep26](https://github.com/JunseokLee3609/VertexCompositeAnalysis/tree/production/dstar-refit-diag-30sep26) | DATA·MC 공통 코드 |

## 묶은 기준

AN DATA와 MC는 보관된 DStarFitter와 PATCompositeTreeProducer6 소스에 차이가 있어 별도 tag로 보존했다.
D0Fitter, TrackAndVertexUnpacker, PATEventPlaneTrack과 BDT 모델은 비교한 두 생산본에서 같았다.
실제 제출 설정의 D0 MVA cut은 DATA 0.9, MC -1이다.

Before/After의 DATA·MC는 사용자의 “mc도 같아 그냥 그걸로해” 확인에 따라 DATA 생산 소스로 하나의 tag에 묶었다.
DATA 제출 PSet과 MC task metadata를 함께 보존했다. 원본 MC 제출 PSet은 확보하지 못했다.
DATA 제출 PSet의 D0 MVA cut은 0.8이다. 이는 MC의 제출 cut을 의미하지 않는다.

## 원본 및 설정 위치

각 tag의 production_snapshot/manifest.json에 소스 기준 commit, 원본 sandbox 경로와 복원 범위가 있다.
production_snapshot/submitted/에 확보한 실제 제출 PSet을 보존했다.
production_snapshot/input_links.json은 해당 생산본과 AN 입력 또는 CRAB task를 연결한다.
참조 ONNX 모델도 각 tag의 원래 상대 경로에 포함했다.

AN DATA는 PD8–15와 PD16–23의 실제 제출 PSet을 확보했다.
PD0–7과 PD24–31은 task 경로를 기록했으며 해당 제출 PSet은 확보하지 못했다.
AN MC의 prompt pT0 설정은 보조 기록이며 AN nominal 6개 샘플에는 포함하지 않는다.

## MB resolution 생산본

기존 HIMinimumBias 18Mar26_v3와 11월 7일 RawPrime 0.4% sampling 중 보존할 제출본 확인이 남아 있다.
확인 후 실제 제출 당시 코드와 설정을 AN DATA와 비교해 공통 tag 또는 별도 tag로 연결한다.
나중에 추가된 ResolutionHFInfo를 과거 생산 소스로 대체해 넣지 않는다.

## AFS 보관 위치

/afs/cern.ch/user/j/junseok/analysis/production_studies/production_versions_20261108

- versions/: 생산본별 소스와 제출 설정
- records/: 입력 연결, 비교 결과, Git 게시 기록
- catalog/: 이 목록과 기계 판독용 catalog.json
- code/: 보존 절차에 사용한 스크립트

기존 producer 작업 branch와 수정 중인 파일은 유지했다.
