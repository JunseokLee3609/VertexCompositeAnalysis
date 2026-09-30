# Production 점검 항목 및 수정 이력 — 2026-09-29

바로 이동: [수정 목록](#전체-목록) · [조사 결과·현재 결론](#current-findings) · [전체 원문 목차](#source-catalogue) · [문서 대조 검증](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/comprehensive_record/validation.json)

이 문서는 **현재 수정 목록 + 조사 결과 대장 + 전체 조사 원문 + 수정/검증 증거**를 한 파일에 담은 통합본이다. 링크만 남기지 않고 아래 원문을 본문에 수록했다.


이 문서는 사용자가 후속 수정을 판단하기 위한 작업 목록이다. **현재 소스/설정 수정 완료: EP-01(PAT6 tracker p/m), EV-01/02/04(data Step2 2곳 + MC Step2 2곳), FIT-03(최초/내부 D0 daughter mass sigma 정렬, 기존 소스 반영 확인), FIT-04(production/Condor raw=True 설정 반영·6경로 cfg load 통과), FIT-05(DStarFitter duplicate slow veto를 무조건 기본 적용), NT-05(PAT6 event scalar 갱신을 missing collection return 앞으로 이동), DIAG-01(성공 D* 후보 전후 진단 46개 branch 추가). Data/MC 설정 로드 검사는 통과했으나 실제 event 처리·새 production 검증은 미완료다. FIT-05는 Fix05에서 무조건 veto로 변경했고 CMSSW GCC syntax-only 검사를 통과했다. 라이브러리 build/event 검증은 아직 하지 않았다. NT-05는 GCC syntax-only 통과; 수정 library build/event 재검증은 미실행이다. 44개 branch는 과거 미구현 제안이다. 이번 DIAG-01은 별도 46개 명세로 소스 반영했으며 event64·실패 pair 저장을 제외했다. Data D* 147→193 / MC D* 249→295; syntax-only 및 native covariance/serialization 검사는 통과했으나 새 module build/cmsRun은 하지 않았다.** 2026-09-30 준석의 결정으로 EV-03은 기존 32-bit 저장을 유지하고, FIT-01/02는 기존 pseudotrack covariance를 그대로 사용한다. FIT-02의 χ²/ndf/finite 검사 문제는 별도 미확정 항목이다. FIT-06은 준석이 추가 실패/탈락 pair 진단 저장을 도입하지 않고 현재 저장 방식 유지로 결정했다. FIT-07은 이미 수행한 수렴·seed 비교를 근거로 현재 기본 설정 유지로 결정하고 추가 튜닝 검토를 종료했다. 별도로 기존 EOS D03의 XY track error 정의 7곳을 사용자 지정 CmsHI reference로 AFS에 반영했고 네 소스 syntax-only를 통과했다. 아래에서 수정 반영·기존 진단으로 해결 13개, 유지 결정 12개, 정의·산술 검증 완료 1개, 나머지 조사/설계 항목 2개를 분리했다. EP-04/05는 준석이 기존 수정 완료를 확인하여 종료했다. NT-01은 DIAG-01의 D*·D0·slow pion Before/After 저장으로 해결 완료, NT-02는 DIAG-01의 별도 fitted 오차/covariance 추가로 해결 완료, NT-03은 기존 2D/3D DCA 정의 유지로 결정하여 잔여 목록에서 제외했다. MVA-01은 D12 부분의 AFS 소스 수정·syntax-only 검증을 완료했으며 과거 학습 provenance 범위는 남아 있다. CFG-01은 현재 production에서 사용하지 않는 경로를 유지하고, CFG-02는 준석의 지시대로 기존 DATA MVA > 0.9 / MC MVA > -1 설정 유지로 종료했다. 그 외 항목은 미수정이며, 확인된 코드 결함·저장 정의·설정 차이·아직 측정하지 않은 위험을 구분했다. 목록 전체를 중앙 Δm peak의 원인으로 판정한 것이 아니다.

앞선 read-only 감사 보고서는 수정 전 상태의 기록으로 유지한다. 현재 상태는 이 목록과 수정 diff를 기준으로 확인한다. 이후 수정도 항목 ID, 수정 파일, 해당 작업 직전→직후 diff, 확인 결과를 항목별로 추가한다. 기존 사용자 변경 사항이 있으므로 git HEAD 전체 diff를 이번 수정 diff라고 부르지 않는다.

## 전체 목록

|ID|현재 상태|분류|항목|확인한 사실과 후속 검토|근거|
|---|---|---|---|---|---|
|EP-01|수정 완료 — 소스만|확인된 연결 오류|PAT6 tracker p/m 반대 연결|p=5/11, m=4/10으로 40개 대입 수정. 아래 diff 참조.|[PATCompositeTreeProducer6.cc:526](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc:526)|
|EV-01|data 2곳 + MC 2곳 설정 수정; event 처리 미검증|확인된 설정/구현 불일치|EventInfo 첫 HLT 문자열의 별표|EventInfo substring 검색에 맞춰 첫 기록 문자열 _v*→_v 수정. 실제 hltHighLevel의 _v*는 유지. EventInfo C++ 수정은 없음.|[PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:360](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:360); [EventInfoTreeProducer.cc:226](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/EventInfoTreeProducer.cc:226)|
|EV-02|data 2곳 + MC 2곳 설정 수정; event 처리 미검증|확인된 설정/기록 불일치|기록 요청한 HF filter 미실행|수정 전 HF flag는 기록 목록에만 있고 미실행이었다. 현재 data/MC 4개 진입 설정은 HF path/schedule 활성화 및 독립 기록.|[PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:366](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:366); [PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:390](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:390); [EventInfoTreeProducer.cc:267](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/EventInfoTreeProducer.cc:267)|
|EV-03|결정 — 기존 32-bit 유지|사용자 유지 결정; 제한된 원본 검사 완료|32-bit EventNb 유지|준석이 64-bit 추가/교체안을 채택하지 않기로 결정. 다른 공개 VertexCompositeAnalysis도 uint + ROOT /i. 원본 MiniAOD 107파일·9,065,159 events에서 32-bit 초과 0; 전체 32 stream 전수 검사는 아님. 아래 결정 근거 참조.|[PATCompositeTreeProducer6.cc:723](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc:723); [PATEventPlaneTrack.cc:851](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATEventPlaneTrack.cc:851); [EventInfoTreeProducer.cc:359](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/EventInfoTreeProducer.cc:359)|
|EV-04|data 2곳 + MC 2곳 설정 수정; event 처리 미검증|선택 의미 확인 필요|colEvtSel 공백과 별도 PV flag|수정 전 Flag_colEvtSel은 사실상 HLT였고 PV/cluster는 별도 flag였다. 현재 data/MC 4개 진입 설정은 세 offline filter를 DStar path에 적용. 과거 ROOT는 변경되지 않음.|[PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:390](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:390)|
|FIT-01|결정 — 기존 pseudotrack covariance 유지|사용자 유지 결정; 물리적 정상성 판정 아님|기존 covariance 입력·전파 유지|준석의 결정대로 기존 pseudotrack covariance를 그대로 사용. 새 PSD gate, clipping, regularization, 대체 covariance는 채택하지 않음. Indefinite covariance의 허용 경로와 valid 판정의 한계라는 기존 관측은 보존.|[DStarFitter.cc:619](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:619); 기존 감사 보고서 §3A|
|FIT-02|결정 — covariance 유지; χ²/finite 검토 별도|covariance 결정과 검사 공백을 구분|기존 covariance 유지 및 χ²/ndf/finite 미확정|기존 pseudotrack covariance 사용 결정. 별도의 χ²/ndf/finite 검사 변경은 결정하지 않음. D0 음수 χ² veto 주석 및 finite/positive 검사 공백의 발생률도 미측정. covariance 유지가 이 검사 공백의 해결을 뜻하지 않음.|[D0Fitter.cc:471](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/D0Fitter.cc:471); [DStarFitter.cc:645](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:645); [DStarFitter.cc:752](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:752)|
|FIT-03|기존 소스 반영 확인 — 완료; 잔여 판단 종료|최초/내부 D0 daughter mass sigma 정렬|π=3.5e-7, K=1.6e-5 GeV로 동일|준석이 기존 결정을 재확인. 현재 AFS의 최초 D0 fit과 내부 D0 refit은 입자별 동일 sigma를 factory에 전달한다. 이번 작업은 오래된 미수정 표기 정정이며 소스 변경 없음. 기존 syntax-only 통과 hash와 현재 소스 일치; build/event 검증 별도.|[현재 소스](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:581); [완료 상태 정정](#fit03-closure-20260930)|
|FIT-04|설정 반영 완료 — raw=True; CMSSW cfg load 6경로 통과|사용자 raw 선택 및 production 설정 적용|D* parent 운동학·cut에 raw 사용|production DATA/MC 2개 및 Condor Step2 cfg 3개에 useRawDStarKinematics=True 반영. D* pT·y·mass cut과 parent p4는 입력 D0+original slow pion 사용. Fit 유효성·vertex 경로와 기존 daughter 저장은 유지. 새 build/cmsRun event/production 검증은 별도.|[설정 diff·검증](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fit04_raw_production_configuration_20260930/REPORT.md)|
|FIT-05|Fix05 기본 veto 소스 적용; syntax-only 통과; build/event 미검증|확인된 로직 결함 수정 및 기본 동작 변경|D0 daughter TrackRef의 slow-pion 재사용 거부|D0 daughter 직후 동일 TrackRef면 무조건 continue. Veto 옵션·중복 debug counter 제거 및 canonical MC False 설정 삭제. Data/MC 모두 소스 기본 적용. 실제 이벤트 영향 미측정.|[DStarFitter.cc:440](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:440)|
|FIT-06|결정 — 현재 저장 방식 유지·추가 진단 저장 미채택|사용자 유지 결정; 영향의 작음이 실측으로 입증된 것은 아님|실패/탈락 pair의 별도 진단 저장 도입 안 함|준석이 현재 작업에서 추가 진단 필요성을 높게 보지 않아 기존 저장 범위를 유지하기로 결정. 기존 prefit gate와 fit 실패 continue, 성공 후보 저장 방식 유지. 별도 실패 pair collection/넓은 진단 fit 제안은 미채택. 저장 한계와 기존 코드 사실은 조사 기록으로 보존.|[DStarFitter.cc:491](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:491); [내부 D0 fit:587](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:587); [DStar fit:604](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:604); [최종 저장:816](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:816)|
|FIT-07|결정 — 조사 완료·현재 수렴/seed 기본 설정 유지|수행한 표본의 수치 검증과 유지 결정|수렴 기준과 시작 vertex seed 유지|1,264후보×15설정 비교 완료. 반복 한도만 100→300이면 동일; XYZ 1μm에서도 PR pass 중앙 비중 불변. 기본 설정을 변경할 근거가 없어 준석의 후속 지시에 따라 추가 튜닝 검토 종료. 기존 예외 관측은 조사 기록으로 보존.|[기존 stability report](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/fit_stability_20260929/REPORT.md)|
|NT-01|해결 완료 — DIAG-01 Before/After 저장|기존 nominal 저장 정의 유지·별도 상태 저장 요구 해결|D*·D0·slow pion의 original/fitted 상태 모두 접근 가능|준석이 DIAG로 해결됐음을 재확인. 저장되는 성공 D* 후보에 대해 Dstar/D0/SlowPi Before/After 6상태의 Pt/Eta/Phi/Y/Mass/PtErr/CovP4를 DATA/MC 공통 경로로 저장하도록 소스 반영돼 있다. Original slow와 fitted slow는 별도 branch로 접근 가능하므로 nominal daughter 통일 추가 판단을 종료한다. 수정 library build/event 검증은 DIAG 공통 잔여.|[상태 정정](#nt01-diag-closure-20260930)|
|NT-02|해결 완료 — DIAG-01 별도 오차 추가|기존 original 오차와 fitted 오차 분리 저장|Fitted momentum과 original track error|기존 pTerrD2 등 original TrackRef 오차는 유지. DIAG-01에 D*·D0·slow pion Before/After PtErr·CovP4를 별도로 추가하여 해당 저장 요구를 해결했다. 준석의 확인에 따라 추가 정의 변경/명시 판단은 종료. DIAG-01의 수정 후 runtime 검증은 별도 공통 항목이다.|[PATCompositeTreeProducer6.cc:1597](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc:1597)|
|NT-03|결정 — 기존 DCA 정의 유지|사용자 유지 결정|DStar-mode DCA의 대상|준석의 지시대로 3D DCA는 D0 daughter userFloat, dca2D는 parent geometry인 기존 정의를 유지. 계산/branch 변경을 도입하지 않고 추가 판단을 종료.|[PATCompositeTreeProducer6.cc:1565](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc:1565)|
|NT-04|결정 — 기존 상한 유지·추가 조사 종료|사용자 유지 결정; 대표 output의 상한 미도달 확인|PAT6/EP candidate cap 유지|준석의 지적에 따라 기존 output 확인. March2026 4개 stream 묶음의 대표 12파일·194,677 저장 events에서 D* 최대 262, D0 최대 36, candSize≥50,000은 0건. 해당 표본에서 cap에 의한 잘림 없음. 전체 production 전수 검사는 아니며 cap 정의 차이라는 코드 사실은 보존.|[candidate cap 측정](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/nt04_nt05_check_20260930/nt04_candidate_cap.json)|
|NT-05|수정 완료 — 소스만; syntax-only 통과|collection 누락 시 PV scalar 재사용 경로 수정|Event scalar 갱신을 missing CCC return 앞으로 이동|준석 승인대로 centrality·EP·PV/track 갱신 호출 3개를 CCC 유효성 검사 앞으로 이동. 기존 2-event runtime 재현은 수정 전 library의 증거다. 수정 후 GCC syntax-only 및 호출 순서 검증 통과. Build와 수정 library의 event 재검증은 미실행.|[수정 diff](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/nt05_scalar_update_20260930/nt05_scalar_update.diff); [검증](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/nt05_scalar_update_20260930/verification.json)|
|MC-01|미수정|Matching 정의/검증 항목|GEN match와 중복|저장된 reco daughter 상태로 matching; 전역 one-to-one 아님. 새 fit을 적용하면 기존 matchGEN을 보존하고 새 matching과 비교해야 함.|[PATCompositeTreeProducer6.cc:1283](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc:1283); [PATCompositeTreeProducer6.cc:1403](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc:1403)|
|CFG-01|결정 — 현재 비활성 경로 유지; 잔여 판단 종료|현재 production 경로 영향 없음 확인|별도 D* MVACollection의 label 차이|보존 DATA/MC PSet 모두 별도 D* collection 읽기는 useAnyMVA=false. D0 ONNX는 활성 상태로 D0mva를 통해 전달·저장되므로 해당 label 차이의 현재 영향 없음. 현재 비활성 경로를 유지하고 추가 label 수정안을 채택하지 않음.|[결정·검증](#cfg-production-retain-20260930)|
|EP-02|결정 — 현재 event별 daughter union 제외 유지|사용자 유지 결정; candidate별 기능은 추후|Custom tracker EP의 후보집합 의존성|준석의 지시대로 event 안의 후보 daughter TrackRef 합집합을 제외하는 현재 방식을 유지. Candidate별 daughter 제외 Q는 추후 추가할 기능으로 분리. 후보 집합 의존성이라는 코드 사실은 보존하며 영향이 작다는 실측 판정으로 바꾸지 않음.|[PATEventPlaneTrack.cc:423](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATEventPlaneTrack.cc:423); [PATEventPlaneTrack.cc:658](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATEventPlaneTrack.cc:658)|
|EP-03|검증 완료 — 정의·산술; 소스 미수정|현재 Q 계산의 오류 근거 없음|q normalization 및 flat angle/Q 구분|현재 소스·CMSSW accessor 확인 및 기존 library의 MiniAOD 10-event 검사. 910 선택 tracks, 9개 영역의 Q₂/Q₃ 재계산 최대 차이 1.28e-7. Official recentered Q와 flattened angle 구분 확인. 전체 production/물리 보정 QA 완료라는 뜻은 아님.|[EP 검증 결과](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/ep03_ep04_producer_validation_20260930/REPORT.md)|
|EP-04|수정 완료 — 준석 확인|기존 수정 완료 확인에 따른 목록 정정|EP 입력·SumW·HeavyIonRPRcd|준석이 이미 수정한 항목임을 확인하여 남은 판단 목록에서 제외. 기존 PbPb2018/75x payload 관측은 수정 전 조사 이력으로 보존하며 현재 미완료 상태로 표시하지 않는다. 이번 작업은 상태 정정이며 새 보정 변경/검증을 수행한 작업이 아니다.|[완료 상태 정정](#fixlist-closure-20260930)|
|EP-05|수정 완료 — 준석 확인|기존 수정 완료 확인에 따른 목록 정정|Official EP에서 empty PV|준석이 이미 수정한 항목임을 확인하여 남은 판단 목록에서 제외. Empty-PV 직접 접근에 대한 수정 전 관측은 조사 이력으로 보존하며 추가 발생/처리 판단을 요청하지 않는다. 이번 작업에서 새 코드 수정/실행 검증을 수행한 것은 아니다.|[완료 상태 정정](#fixlist-closure-20260930)|
|MVA-01|D12 AFS 소스 수정 완료 — syntax-only 통과; 기타 provenance 별도|정상 값 정의 대조 완료; 저장/추론 예외 표현 통일|20 ONNX feature mapping 및 D12 significance/VtxProb 공통값|기존 EOS D12를 AFS의 공통 header·D0Fitter·PAT6에 반영. 무효 significance는 NaN, D0 VtxProb는 유효 chi2=0일 때 1. ONNX/ntuple은 같은 userFloat 사용. GCC syntax-only 2개 통과. 라이브러리 build/event 재검증 미실행. 과거 March training centrality provenance는 별도 잔여 범위. ONNX·학습 데이터 미변경.|[적용 diff](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/mva01_D12_afs_port_20260930/D12_afs_port.diff); [검증](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/mva01_D12_afs_port_20260930/verification.json); [20개 대조·이식 기록](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/mva01_feature_definition_check_20260930/REPORT.md)|
|CFG-02|결정 — 기존 DATA/MC production 설정 유지|사용자 유지 결정; 보존 PSet 대조 완료|DATA D0 MVA > 0.9 / MC D0 MVA > -1 유지|준석이 기존 생산 설정을 그대로 사용한다고 확정. 보존 DATA RawPrime8–15/16–23 PSet은 0.9, official MC PR pT4/NPR pT4/PR pT0 PSet은 -1 확인. 다른 현재 cfg 차이는 조사 이력으로 보존하고 기존 production 선택의 변경 검토 종료.|[결정·검증](#cfg-production-retain-20260930)|
|OPS-01|결정 — 기존 job 실행·로그 방식 유지|사용자 유지 결정|현재 AFS runtime과 고정 Condor 로그 이름 유지|준석의 지시대로 source/library snapshot 추가와 job별 로그 이름 변경을 도입하지 않고 기존 방식을 유지. 기존 운영 관측은 조사 기록으로 보존.|[submit_condor.sh:152](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/submission/condor/submit_condor.sh:152)|
|COV-01|결정 — 기존 covariance 입력·변환 유지|사용자 유지 결정|기존 track→transient→kinematic 변환 유지|준석의 지시대로 기존 covariance 입력·전파와 CMSSW 표준 변환을 유지. 5×5/7×7 정의와 조건부 recoverTracks 경로의 관측은 조사 기록으로 보존.|[TrackAndVertexUnpacker.cc:75](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/plugins/TrackAndVertexUnpacker.cc:75); [TrackAndVertexUnpacker.cc:101](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/plugins/TrackAndVertexUnpacker.cc:101)|
|DIAG-01|수정 완료 — 소스만; syntax-only·수치/serialization 검사 통과|성공 후보의 전후 상태·오차 진단 추가|D*·D0·slow pion의 Before/After 운동학·pT error·covariance|46개 branch 추가: data D* 147→193, MC D* 249→295. 기존 nominal/cut/matching/covariance 입력은 유지. Event64와 실패/탈락 pair 저장 제외. 실제 수정 module build/cmsRun/production QA는 미실행.|[전체 diff·branch 명세·검증](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/diag01_success_before_after_20260930/REPORT.md)|

### AFS 작업 체크 — 2026-09-30

- [x] D12: significance/VtxProb 공통값 AFS 소스 반영 및 문법 검사 완료.
- [x] 기존 CmsHI XY reference: D0·D*·Ntrk·custom EP 활성 계산 7곳 AFS 반영, 네 소스 문법 검사 완료.
- [ ] 위 수정의 library build 및 수정 후 event/production 검증.

## 결정된 사항과 추가 조사 — 2026-09-30

이 절이 현재 작업 상태다. **수정 방향 결정, 소스/설정 반영, 실제 event 처리 검증을 구분한다.** 아래 유지 결정은 준석의 명시적 지시이며, 기존 감사에서 관측한 수치 이상이 없었다는 판정으로 바꾸지 않는다.

### A. 수정 반영·기존 진단으로 해결한 항목 — 13개

|ID|결정 및 반영 내용|남은 검증|
|---|---|---|
|EP-01|PAT6 tracker p/m 연결 수정|수정 library build와 event 확인|
|EV-01|EventInfo의 첫 HLT 기록 문자열 `_v*`→`_v`|실제 event 기록 확인|
|EV-02|HF filter 실행과 독립 flag 기록|실제 DATA/MC cutflow 확인|
|EV-04|PV·cluster compatibility·HF를 DStar path의 offline selection에 적용|실제 event 손실률과 GEN 분모 영향 확인|
|FIT-03|기존 결정·소스 반영 확인: 최초/내부 D0 fit 모두 π sigma=3.5e-7, K sigma=1.6e-5 GeV 사용; 오래된 미수정 표기 정정|현재 소스가 기존 syntax-only 통과 hash와 일치; 공통 build/event 검증은 별도|
|FIT-04|production DATA/MC 및 Condor Step2 cfg 5개에 raw=True 반영; dual-mode 포함 6개 CMSSW cfg load 통과|현재 cut·daughter 저장 유지; 새 event/production 실행 검증은 별도|
|FIT-05|D0 daughter TrackRef를 slow pion으로 재사용하면 무조건 veto|소스 syntax-only 통과; build/event 및 후보 영향 미검증|
|NT-05|CCC 누락으로 return하기 전에 centrality·EP·PV/track 정보 갱신|GCC syntax-only 통과; 수정 library build 및 2-event 재검증 미실행|
|DIAG-01|성공 후보 D*·D0·slow pion Before/After 운동학·오차·covariance 46개 branch 추가; event64/실패 pair 제외|GCC syntax-only 및 8개 native covariance/serialization 사례 통과; 새 module build/cmsRun 및 실제 후보 QA 미실행|
|NT-01|DIAG-01의 Dstar/D0/SlowPi Before/After 6상태 저장으로 해결; nominal daughter 정의 유지|소스 선언·값 전달 확인; DIAG-01 공통 수정 library build/event 검증은 별도|
|NT-02|DIAG-01의 별도 fitted PtErr/CovP4 추가로 해결; 기존 original track-error 유지|DIAG-01 공통 runtime 검증만 별도; 추가 정의 변경 판단 종료|
|EP-04|준석이 기존 수정 완료 확인; 잔여 판단 종료|이번 기록에서 새 calibration 검증을 주장하지 않음|
|EP-05|준석이 기존 수정 완료 확인; 잔여 판단 종료|이번 기록에서 새 empty-PV 검증을 주장하지 않음|

### B. 기존 구현을 유지하기로 결정한 항목 — 12개

|ID|결정|결정 근거와 적용 범위|
|---|---|---|
|EV-03|기존 unsigned 32-bit `EventNb`와 ROOT `/i` 유지|다른 공개 VertexCompositeAnalysis의 동일 저장 방식, 제한된 원본 MiniAOD overflow 검사 결과, 기존 저장 형식 유지에 대한 준석의 최종 지시. 64-bit 추가/교체안 미채택.|
|FIT-01|기존 pseudotrack covariance를 그대로 사용|준석의 명시적 결정. 기존 입력 covariance와 현재 fit 전파 경로를 유지하며 clipping·regularization·새 PSD veto·대체 covariance를 채택하지 않음.|
|FIT-02|covariance에 대해서 FIT-01과 같은 유지 결정|준석의 지시는 covariance 유지 범위다. 원래 FIT-02의 χ²/ndf/finite 검사는 별도 문제이며 새로운 검사/선택 변경은 결정하지 않음.|
|FIT-06|현재 저장 방식 유지; 실패/탈락 pair의 별도 진단 저장 미채택|준석이 현재 작업에서 큰 문제로 보지 않아 추가 저장을 도입하지 않기로 결정. 영향이 작다는 실측 결론으로 표시하지 않으며, 기존 저장 한계의 코드 사실은 보존.|
|FIT-07|현재 수렴 기준과 기본 vertex seed 유지; 추가 튜닝 검토 종료|이미 수행한 1,264후보×15설정 비교에서 기준 강화가 중앙 집중을 개선하지 않았음. 준석의 후속 지시대로 미확정 수정 목록에서 제외하고 조사 완료로 기록. 예외 사례와 기존 보고서는 보존.|
|NT-04|기존 PAT6 50,000 / EP 500,000 상한 유지; 추가 조사 종료|준석이 실제 output에서 상한에 도달하지 않음을 지적. 대표 12파일·194,677 events에서 D* 최대 262, D0 최대 36, 상한 도달 0건 확인. 전체 파일 전수검사 완료라는 뜻은 아님.|
|EP-02|현재 event별 후보 daughter TrackRef 합집합 제외 방식 유지|준석의 명시적 지시. Candidate별 제외 Q는 추후 기능으로 분리하며 현재 producer의 제외 알고리즘을 변경하지 않음. 후보 집합 의존성이라는 기존 관측은 보존.|
|OPS-01|현재 AFS runtime 사용과 고정 Condor 로그 이름 유지|준석의 명시적 지시. Source/library snapshot 추가와 job별 로그 이름 변경을 도입하지 않고 기존 방식을 유지.|
|COV-01|기존 covariance 입력·전파와 CMSSW 표준 변환 유지|준석의 명시적 지시. 5×5/7×7 정의와 조건부 recoverTracks 경로는 조사 기록으로 보존.|
|NT-03|기존 2D/3D DCA 대상 정의 유지|준석의 명시적 결정. 3D=D0, 2D=parent인 기존 계산/branch 정의 유지; 추가 판단 종료.|
|CFG-01|별도 D* MVACollection 비활성 경로 유지; 현재 영향 없음|보존 DATA/MC PSet에서 useAnyMVA=false 확인. D0 ONNX 점수의 D0mva 전달·저장 경로는 별도로 활성. 추가 label 수정 검토 종료.|
|CFG-02|기존 DATA D0 MVA > 0.9 / MC D0 MVA > -1 production 설정 유지|준석의 명시적 결정. 기존 생산 PSet 기준으로 유지하며, 현재 대체 cfg와의 차이를 기존 production 오류로 취급하지 않음.|

**FIT-02의 남은 범위:** covariance를 유지한다는 정책은 결정됐다. 음수 χ², ndf≤0, non-finite 값의 실제 발생률과 기록/검사 처리 방식은 아직 확정하지 않았다. 진단 측정과 nominal veto 도입을 동일한 결정으로 취급하지 않는다.

### C. 남은 판단·확인 항목 — 2개

#### 모듈·저장 방식 또는 입력 정의의 판단 — 2개

|ID|판단할 내용|
|---|---|
|MC-01|GEN matching과 후보 중복 정의를 유지할지; 새 상태를 쓰는 경우 matching 대조가 필요한지|
|MVA-01|D12 AFS 소스 반영·syntax-only 검증 및 20개 입력 대조 완료. 과거 March 학습 flat centrality 입력 provenance 대응 범위는 별도 판단. Build/수정 후 event 검증 미실행|

이 2개는 모두 수정해야 할 버그 목록이 아니다. NT-01은 준석의 확인과 DIAG-01의 DATA/MC 공통 Before/After 상태 저장 근거로 해결 완료하여 A절로 이동했다. FIT-04는 준석의 raw 선택에 따라 production/Condor 설정 반영 및 6경로 CMSSW cfg load를 완료하여 A절로 이동했다. CFG-01은 현재 비활성 경로의 영향 없음 확인 및 유지 결정, CFG-02는 기존 DATA/MC production 설정 유지 결정으로 종료했다. FIT-03은 현재 AFS 소스의 입자별 mass sigma 정렬 및 준석의 기존 결정 재확인으로 완료 처리했다. EP-04/05는 준석의 기존 수정 완료 확인으로 종료했고, NT-02는 DIAG-01의 fitted 오차/covariance 추가로 해결 완료, NT-03은 기존 정의 유지 결정으로 종료했다. DIAG-01은 성공 후보의 전후 진단 46개 branch를 소스 반영하여 A절로 이동했다. OPS-01/COV-01은 준석의 기존 유지 결정으로 B절에 이동했다. NT-05는 소스 수정 완료로 A절에 이동했다. EP-03은 정의·산술 검증 완료로 이 목록에서 제외했다. 변경 여부를 판단할 항목과 정의·발생 여부를 확인할 항목을 구분했다. FIT-06은 현재 저장 방식 유지, FIT-07은 현재 기본 수렴/seed 유지, NT-04는 현재 cap 유지, EP-02는 현재 event별 daughter union 제외 유지로 결정해 이 목록에서 제외했다. FIT-02의 covariance 유지 정책도 결정됐으며 χ²/ndf/finite 검사에 대한 잔여 범위는 위에 별도 표시했다.


### D. 정의·산술 검증 완료 — 1개

|ID|확인 결과|범위|
|---|---|---|
|EP-03|Custom q=Q/ΣpT, official q=Q의 크기, recentered Q와 flat angle 정의 확인. 계산 수정 근거 없음.|현재 AFS 소스와 기존 library의 MC 10-event 산술 대조. 후보 집합 영향·전체 production·Run3 calibration 물리 QA 완료 판정은 아님.|

### EP-03/EP-04 과거 producer 검증 — 2026-09-30

[EP 검증 결과](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/ep03_ep04_producer_validation_20260930/REPORT.md)에 현재 소스·세 cfg 로드·기존 library의 실제 MiniAOD 10-event 실행·원본 track 재계산·actual GlobalTag/IOV/payload 검사를 기록했다. **이 당시 검증에서는 EP-03 정의·산술 및 EP-04 입력/harmonic SumW를 확인했고 구형 EP payload가 관측됐다. 이후 준석이 EP-04 수정 완료를 확인했으므로 현재 잔여 판단에는 포함하지 않는다.** Source/config 변경이나 build는 이 검증에서 수행하지 않았다. PAT6 main-task source 변경은 provenance에 따로 표시했다.

### EP-02 결정 — 2026-09-30

준석의 지시대로 현재 event별 후보 daughter TrackRef 합집합 제외 방식을 유지한다. Candidate별 daughter 제외 Q 저장은 추후 추가할 기능으로 구분한다. 이번 결정은 현재 제외 알고리즘의 수정안을 채택하지 않는다는 뜻이며, 후보 집합 변경에 따른 EP 영향이 작다는 수치 검증 결과로 취급하지 않는다. EP-03/04는 producer의 계산·저장·입력 설정을 별도로 검증한다.

<a id="fixlist-closure-20260930"></a>
### EP-04/05·NT-02/03 완료 및 유지 상태 정정 — 2026-09-30

- [x] EP-04/05: 준석이 이미 수정한 항목임을 확인하여 남은 목록에서 제외. 이번 갱신의 근거는 준석의 기존 수정 완료 확인이며, 새 소스 변경이나 calibration/empty-PV 실행 검증을 주장하지 않는다.
- [x] NT-02: DIAG-01에 `diagDstarAfterPtErr`, `diagD0AfterPtErr`, `diagSlowPiAfterPtErr` 및 Before/After `CovP4`를 별도로 추가했으므로 저장 요구 해결 완료. 기존 `pTerr`의 original TrackRef 정의는 유지. 별도의 명시/변경 결정을 추가로 요구하지 않는다.
- [x] NT-03: 준석의 지시대로 3D DCA=D0, 2D DCA=parent인 기존 정의 유지. 추가 판단 종료.

현재 남은 판단·확인은 **MC-01, MVA-01 — 2개**다. FIT-02의 covariance 유지 및 별도 χ²/ndf/finite 범위, 이미 반영한 변경의 공통 build/event 검증 상태는 그대로다. 이번 작업의 수정 파일은 통합 fixlist와 집계 JSON뿐이다.

정적 근거: 현재 PAT6.cc 736–747의 DIAG `PtErr`/`CovP4` branch 선언, 1300–1314의 native P4/covariance 읽기와 pT 오차 계산, D0Fitter.cc의 `diagD0CovP4` 전달 및 DStarFitter.cc의 Before/After state 전달을 확인했다. [DIAG-01 기존 보고서](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/diag01_success_before_after_20260930/REPORT.md). 추가 source/build/cmsRun은 수행하지 않았다.

<a id="fit03-closure-20260930"></a>
### FIT-03 기존 결정·소스 반영 확인 및 상태 정정 — 2026-09-30

- [x] 준석이 기존 결정 완료를 재확인하여 FIT-03을 남은 판단 목록에서 제외.
- [x] 현재 최초 D0 fit과 내부 D0 refit은 모두 π sigma=3.5e-7 GeV, K sigma=1.6e-5 GeV를 입자 종류에 맞춰 factory에 전달한다.
- [x] 기존 표의 “내부 두 daughter 모두 1.6e-4 GeV”는 수정 전 조사 내용이다. 현재 작업 상태 표를 소스와 일치하도록 정정했다.
- [x] D0Fitter.cc와 DStarFitter.cc의 현재 SHA256은 기존 XY reference 작업의 syntax-only 통과 SHA256과 동일하다. 이번 작업에서 새 compile/build/cmsRun은 수행하지 않았다.

현재 남은 항목은 MC-01, MVA-01 — 2개다. FIT-02의 별도 χ²/ndf/finite 범위와 공통 build/event 검증 상태는 유지한다. 기존 조사 원문과 source/config는 보존했다.

[정정 diff·검증](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fixlist_closure_FIT03_20260930/REPORT.md)

<a id="cfg-production-retain-20260930"></a>
### CFG-01/02 기존 production 설정 유지 확정 — 2026-09-30

준석의 결정: 기존 March DATA와 official MC production의 MVA 선택 설정을 그대로 사용한다.

- [x] DATA: D0 ONNX 점수 > 0.9 유지.
- [x] MC: D0 ONNX 점수 > -1 유지. ONNX 추론·점수 저장은 계속 수행하며 정상 0–1 점수에 대한 MVA 선택을 제한하지 않는다.
- [x] CFG-01: 보존 DATA/MC PSet에서 별도 D* MVACollection 읽기는 useAnyMVA=false. D0 점수는 D0mva로 전달·저장된다. 해당 label 차이는 현재 production 경로에 영향 없으므로 현재 비활성 경로 유지로 종료.
- [x] CFG-02: 기존 생산 설정 유지로 확정하여 잔여 판단 목록에서 제외. 다른 cfg의 값 차이는 기존 조사 기록으로 보존한다.

대조 범위는 DATA RawPrime8–15/16–23의 보존 CRAB PSet, official MC PR pT4/NPR pT4/PR pT0의 원래 제출 PSet이다. 모든 DATA stream이나 MC campaign의 전체 결과를 새로 검증했다는 뜻은 아니다. 현재 DATA Condor 대체 cfg의 -1 값을 과거 DATA production 전체에 적용된 값으로 표시하지 않는다. 이번 작업은 결정 기록과 상태 정정이며 소스·configuration·기존 ROOT를 수정하거나 새 build/cmsRun/production을 수행하지 않았다.

현재 잔여 항목은 MC-01, MVA-01 — 2개다. FIT-02의 별도 χ²/ndf/finite 범위와 공통 수정 library build/event 검증 상태는 유지한다.

[결정 기록·diff·검증](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fixlist_closure_CFG01_CFG02_20260930/REPORT.md)

<a id="fit04-raw-production-20260930"></a>
### FIT-04 raw production 설정 반영 — 2026-09-30

준석의 지시대로 기존 useRawDStarKinematics 옵션을 production configuration에서 True로 설정했다.

|cfg|현재 줄|raw 옵션|
|---|---:|---|
|[PbPb2023_D0BothAndDStar_MB_cfg_v2_Step2MVA.py](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/production/pbpb2023/data/PbPb2023_D0BothAndDStar_MB_cfg_v2_Step2MVA.py:200)|200|True|
|[PbPb2023_D0BothAndDStar_MB_cfg_mc_v2_Step2MVA.py](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/production/pbpb2023/mc/PbPb2023_D0BothAndDStar_MB_cfg_mc_v2_Step2MVA.py:227)|227|True|
|[PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:241)|241|True|
|[PbPb2023_D0BothAndDStar_MB_cfg_mc_Step2MVA_Condor_v1.py](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_mc_Step2MVA_Condor_v1.py:229)|229|True|
|[PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_isMC_v1.py](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_isMC_v1.py:222)|222|True|

- [x] cfg 5개는 raw 옵션 한 줄 추가/False→True만 변경. raw assignment를 제거한 Python AST는 작업 전후 동일하다.
- [x] production DATA/MC, 실제 Condor DATA/MC 진입 cfg, dual-mode cfg DATA/MC 모드 총 6경로 CMSSW 설정 로드 통과. Raw=True와 DStar producer path 연결 확인.
- [x] 기존 각 cfg의 MVA cut 및 나머지 선택 값 보존. Canonical production DATA는 0.9, MC는 -1. 기존 대체 DATA Condor/dual-mode cfg의 -1 값도 이 FIT-04 작업에서는 변경하지 않았다. 앞선 nominal DATA/MC production 유지 결정과 대체 cfg 현황을 구분한다.
- [x] D0Fitter/DStarFitter/PAT6 등 C++ source hash 보존. Shared cfi, 보존 CRAB PSet, 기존 ROOT 변경 없음.

Raw parent p4는 최초 D0 fit의 입력 candidate p4와 original slow-pion track p4의 합이다. D* pT·rapidity·최종 mass cut과 저장 parent p4에 이 값을 사용한다. 기존 fit 유효성·vertex 처리 경로를 거치며, stored slow-pion daughter는 final D* fit p4를 유지한다. 이후 준석이 DIAG Before/After로 상태 저장 요구가 해결됐음을 확인하여 NT-01도 해결 완료로 정정했다. DIAG Before/After와 기존 nominal 정의는 함께 보존했다.

검증 범위는 Python 문법/AST·실제 CMSSW configuration 로드다. Build와 cmsRun event 처리, 새 production 및 후보/GEN/EP 영향 검증을 수행하지 않았다. SCRAM의 el8 release/el9 host 안내는 환경 로그에 보존했으며 설정 로드 6경로는 모두 exit 0이었다.

현재 잔여 항목은 MC-01, MVA-01 — 2개다. FIT-02의 별도 χ²/ndf/finite 범위와 공통 수정 library build/event 검증 상태는 유지한다.

[설정 diff·상태 정정·검증](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fit04_raw_production_configuration_20260930/REPORT.md)

<a id="nt01-diag-closure-20260930"></a>
### NT-01 DIAG Before/After로 해결 완료 — 2026-09-30

준석이 original/fitted 상태를 이미 DIAG로 모두 저장하고 있음을 지적했다. 현재 소스 선언과 값 전달을 확인하여 NT-01을 추가 판단 목록에서 제외했다.

- [x] DATA/MC production cfg와 Condor Step2 cfg 모두 raw=True 설정 유지 확인. 직전 CMSSW cfg load 6경로 PASS 기록과 현재 cfg hash 일치.
- [x] DStarFitter.cc 821–835: DstarBefore/After, D0Before/After, SlowPiBefore/After 6상태 모두 storeState로 전달.
- [x] SlowPiBefore는 original slow-pion track p4, SlowPiAfter는 final D* fit의 slow-pion state다. D0Before는 최초 D0 fit candidate이고 D0After는 final D* fit child다.
- [x] PAT6.cc 736–751 및 1300–1320: 각 상태의 Pt/Eta/Phi/Y/Mass/PtErr/CovP4 branch 선언과 값 채움 확인. 조건은 D* 모드(twoLayerDecay 및 abs(pid)=413)이며 DATA/MC 구분 없이 적용된다.
- [x] 기존 nominal daughter branch 정의와 DIAG 상태들을 함께 유지하고 추가 nominal 통일 판단을 종료한다.

검증 범위는 현재 소스/설정 및 직전 cfg load 기록이다. 저장되는 성공 D* 후보의 진단 명세이며 실패·탈락 pair나 raw K/pi 전체 추가 저장을 주장하지 않는다. 수정 library build/cmsRun/production 확인은 DIAG-01 공통 실행 검증으로 별도 유지한다. 이번 작업은 문서·집계 JSON 정정이며 소스/설정/기존 ROOT 변경이나 build/cmsRun을 수행하지 않았다.

현재 잔여 항목은 MC-01, MVA-01 — 2개다. FIT-02의 별도 χ²/ndf/finite 범위와 공통 수정 library build/event 검증 상태는 유지한다.

[정정 diff·검증](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fixlist_closure_NT01_DIAG_20260930/REPORT.md)

### AFS XY track impact-parameter error: 이전 CmsHI reference 반영 — 2026-09-30

#### 결정과 정확한 정의

준석의 요청: “이거 체크해놓고 체크리스트 갱신해놓고 우리 vtx covariance 예전과 정의가 달라진게 있거든? z인가 xy인가 그거 찾아서 고쳐야해 예전버전으로 그게 recommendation이야”.

변경 대상은 **XY track impact-parameter error 분모**다. 기존 EOS D03 결정과 2026-09-10 `dca_reference_update/REPORT.md`에는 준석이 지정한 [CmsHI TrackAnalyzer.cc forest_CMSSW_10_3_1, lines 601–604](https://github.com/CmsHI/cmssw/blob/forest_CMSSW_10_3_1/HeavyIonsAnalysis/TrackAnalysis/src/TrackAnalyzer.cc#L601-L604)가 기록돼 있다. “예전 버전”은 이 사용자 지정 legacy reference로 해석했고 작업 중 준석에게 식을 명시했다. AFS Git HEAD와 이식 직전 소스는 두 인자 covariance overload였으므로 AFS Git HEAD로 단순 되돌린 작업은 아니다.

Reference source의 604줄 중 최소 인용: `sqrt(etrk.dxyError()*etrk.dxyError()+pev_.xVtxErr[pev_.maxPtVtx]*pev_.yVtxErr[pev_.maxPtVtx])`.

적용한 정의:

```cpp
sqrt(track.dxyError() * track.dxyError() + reference.xError() * reference.yError())
```

z 오차 식은 두 버전에서 `sqrt(track.dzError()^2 + reference.zError()^2)`로 같았고 변경하지 않았다. Fitted D0/D* decay vertex 및 decay-length covariance 계산도 변경하지 않았다. PV/beam-spot 선택과 cut threshold/inequality, D12 significance/VtxProb 공통값 경로는 보존했다.

#### 적용 위치 — 4파일, 활성 계산 7곳

|파일|현재 줄|역할|
|---|---:|---|
|[D0Fitter.cc](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/D0Fitter.cc:270)|270|D0 track preselection|
|[D0Fitter.cc](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/D0Fitter.cc:347)|347|D0 positive daughter error/significance|
|[D0Fitter.cc](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/D0Fitter.cc:354)|354|D0 negative daughter error/significance|
|[DStarFitter.cc](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:398)|398|D* slow-pion track preselection|
|[DStarFitter.cc](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:780)|780|D* stored slow-pion error/significance|
|[PATCompositeTreeProducer6.cc](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc:696)|696|PAT6 Ntrkoffline track selection|
|[PATEventPlaneTrack.cc](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATEventPlaneTrack.cc:595)|595|custom EP track selection|

두 fitter에서 더 이상 사용하지 않는 `bestvtxCov` 지역 선언만 제거했다. DStarFitter의 비활성 주석은 변경하지 않았다. 다른 코드나 새 fallback을 추가하지 않았다. 공식 CMSSW EvtPlaneProducer는 이번 사용자 지정 D03의 변경 대상이 아니다. [CMSSW_13_2_11 official EP source](https://github.com/cms-sw/cmssw/blob/CMSSW_13_2_11/RecoHI/HiEvtPlaneAlgos/src/EvtPlaneProducer.cc#L533)는 기존 covariance overload를 계속 사용한다. 사용자 선택 reference와 framework API를 동일한 식으로 표시하지 않는다.

#### 검증과 실행 경계

- 네 실제 translation unit 모두 CMSSW GCC `-fsyntax-only` exit 0; 검사 중 소스 hash 동일.
- 저장된 before/after diff의 whitespace 검사 통과. 활성 7개 식을 실제 현재 소스에서 확인.
- D12 header는 EOS와 같은 SHA256이며 ONNX/PAT6 공통 userFloat 매핑 유지 확인.
- z error와 fitted decay-length covariance 문장이 before/after에서 동일.
- Producer/analyzer library 4개 hash는 검사 전후 동일. Build·cmsRun·재학습 미실행.
- D0/D* 기존 미사용 변수 경고는 로그에 보존. PAT6/custom EP 성공 log는 비어 있음.
- 첫 custom EP 검사 명령은 UTM include 누락으로 실패했다. 실제 기존 build `.d` 기록의 include를 추가한 명령으로 custom EP만 재검사하여 통과했다. 코드 우회나 dependency fallback은 추가하지 않았다.
- 이번 AFS 복원의 후보 수·Ntrk·EP 영향은 event 실행으로 측정하지 않았다. 과거 EOS 비교 결과는 해당 표본/시점의 기록이며 현재 AFS production 검증으로 재사용하지 않는다.

[전체 source diff](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/xy_dca_CmsHI_reference_restore_20260930/xy_dca_reference_restore.diff) · [검증 JSON](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/xy_dca_CmsHI_reference_restore_20260930/verification.json) · [compiler 결과](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/xy_dca_CmsHI_reference_restore_20260930/syntax_check_result.json)

#### 적용 diff

```diff
--- a/VertexCompositeProducer/src/D0Fitter.cc
+++ b/VertexCompositeProducer/src/D0Fitter.cc
@@ -243,3 +243,2 @@
   math::XYZPoint bestvtx(xVtx,yVtx,zVtx);
-  const auto bestvtxCov = (isVtxPV ? vtxPrimary->covariance() : theBeamSpotHandle->rotatedCovariance3D());
 
@@ -270,3 +269,3 @@
       double dzerror = sqrt(tmpRef->dzError()*tmpRef->dzError()+zVtxError*zVtxError);
-      double dxyerror = tmpRef->dxyError(bestvtx, bestvtxCov);
+      double dxyerror = sqrt(tmpRef->dxyError()*tmpRef->dxyError()+xVtxError*yVtxError);
 
@@ -347,3 +346,3 @@
       double dzerror_pos = sqrt(positiveTrackRef->dzError()*positiveTrackRef->dzError()+zVtxError*zVtxError);
-      double dxyerror_pos = positiveTrackRef->dxyError(bestvtx, bestvtxCov);
+      double dxyerror_pos = sqrt(positiveTrackRef->dxyError()*positiveTrackRef->dxyError()+xVtxError*yVtxError);
       double dauLongImpactSig_pos = dzvtx_pos/dzerror_pos;
@@ -354,3 +353,3 @@
       double dzerror_neg = sqrt(negativeTrackRef->dzError()*negativeTrackRef->dzError()+zVtxError*zVtxError);
-      double dxyerror_neg = negativeTrackRef->dxyError(bestvtx, bestvtxCov);
+      double dxyerror_neg = sqrt(negativeTrackRef->dxyError()*negativeTrackRef->dxyError()+xVtxError*yVtxError);
       double dauLongImpactSig_neg = dzvtx_neg/dzerror_neg;
--- a/VertexCompositeProducer/src/DStarFitter.cc
+++ b/VertexCompositeProducer/src/DStarFitter.cc
@@ -372,3 +372,2 @@
   math::XYZPoint bestvtx(xVtx,yVtx,zVtx);
-  const auto bestvtxCov = (isVtxPV ? vtxPrimary->covariance() : theBeamSpotHandle->rotatedCovariance3D());
 
@@ -398,3 +397,3 @@
       double dzerror = sqrt(tmpRef->dzError()*tmpRef->dzError()+zVtxError*zVtxError);
-      double dxyerror = tmpRef->dxyError(bestvtx, bestvtxCov);
+      double dxyerror = sqrt(tmpRef->dxyError()*tmpRef->dxyError()+xVtxError*yVtxError);
 
@@ -780,3 +779,3 @@
        const double slowPiDzErr = sqrt(pionTrackRef->dzError() * pionTrackRef->dzError() + zVtxError * zVtxError);
-       const double slowPiDxyErr = pionTrackRef->dxyError(bestvtx, bestvtxCov);
+       const double slowPiDxyErr = sqrt(pionTrackRef->dxyError() * pionTrackRef->dxyError() + xVtxError * yVtxError);
        const double slowPiDzSig = slowPiDz / slowPiDzErr;
--- a/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc
+++ b/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc
@@ -695,3 +695,3 @@
       const double dzerror = std::sqrt(trk.dzError() * trk.dzError() + bestvzError * bestvzError);
-      const double dxyerror = trk.dxyError(bestvtx, vtx.covariance());
+      const double dxyerror = std::sqrt(trk.dxyError() * trk.dxyError() + vtx.xError() * vtx.yError());
       if (!trk.quality(reco::TrackBase::highPurity)) continue;
--- a/VertexCompositeAnalyzer/plugins/PATEventPlaneTrack.cc
+++ b/VertexCompositeAnalyzer/plugins/PATEventPlaneTrack.cc
@@ -594,3 +594,3 @@
         float dzerror = sqrt(track->dzError()*track->dzError()+bestvzError*bestvzError);
-        float dxyerror = track->dxyError(bestvtx, vtx.covariance());
+        float dxyerror = sqrt(track->dxyError()*track->dxyError()+bestvxError*bestvyError);
```

### MVA-01: ONNX 20개 입력 정의 대조 — 2026-09-30

#### 결론

**20개 이름·순서는 native 모델, 현재 loader, AFS Data Condor/canonical 및 MC canonical 설정에서 모두 일치한다.** 현재 featureValue mapper가 20개 이름 모두를 인식하므로 이 설정의 입력이 unknown-name default 0으로 떨어지지 않는다. D1/D2는 양전하/음전하 D0 daughter 순서이며 고정 K/π 순서가 아니다. pTerrD1/2는 원래 track의 절대 ptError(), dEta_dau는 EtaD1−EtaD2이며 부호를 유지한다.

**AFS D12 소스 반영 완료; GCC syntax-only 통과.** 2026-09-30 준석의 지시로 기존 EOS D12 significance/VtxProb 공통 정의를 AFS에 적용했다. 무효 significance는 NaN이며 저장·ONNX 입력이 같은 userFloat를 쓴다. D0 VtxProb 및 D* 안 D0 VtxProb도 공통 저장값을 사용한다. 20개 입력 이름·순서와 정상 값 정의 대조 결과는 유지한다. 라이브러리 build와 수정 후 event 검증은 미실행이다. 과거 학습 flat centrality event 연결은 별도 provenance 범위이며 모델·데이터는 변경하지 않았다.

#### AFS D12 소스 반영 — 2026-09-30

준석의 요청 “D12는 또 뭐야 afs에 반영해서 diff를 보여줘”에 따라 아래 세 파일을 수정했다.

- D0TrainingFeatures.h: EOS와 byte-identical 공통 함수. 유효 L/sigma, 무효 NaN; 유효 chi2>=0/ndof>0의 TMath::Prob, chi2=0이면 1.
- D0Fitter.cc: 공통 significance/VtxProb를 한 번 계산해 userFloat에 저장하고 ONNX가 해당 userFloat를 읽음.
- PATCompositeTreeProducer6.cc: D0 parent 및 D* 안 D0의 VtxProb를 공통 userFloat에서 읽음.

현재 topology/preselection cut 식과 다른 작업의 진단 필드는 보존했다. 새 ntupler의 D0 VtxProb 경로는 새 D0 producer의 userFloat를 요구한다. 이전 EDM에 이 필드가 없는 경우를 위한 fallback은 추가하지 않았다.

두 실제 translation unit의 CMSSW GCC `-fsyntax-only` exit code는 0이다. D0Fitter의 기존 미사용 변수 경고는 로그에 보존했고 PAT6 log는 비어 있다. 공통 header SHA256은 검증된 EOS와 정확히 같다. 기존 EOS 경계조건 18개는 과거 동일 helper의 검증이며 이번에 재실행한 것으로 표시하지 않는다. 검사 전후 라이브러리 4개 hash는 동일하다. Build·cmsRun·재학습은 수행하지 않았다.

[전체 diff](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/mva01_D12_afs_port_20260930/D12_afs_port.diff) · [소스/문법 검증](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/mva01_D12_afs_port_20260930/verification.json) · [컴파일러 결과](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/mva01_D12_afs_port_20260930/syntax_check_result.json)

적용한 소스 변경:

```diff
--- a/VertexCompositeProducer/src/D0Fitter.cc
+++ b/VertexCompositeProducer/src/D0Fitter.cc
@@ -17,2 +17,3 @@
 #include "VertexCompositeAnalysis/VertexCompositeProducer/interface/D0Fitter.h"
+#include "VertexCompositeAnalysis/VertexCompositeProducer/interface/D0TrainingFeatures.h"
 #include "VertexCompositeAnalysis/VertexCompositeProducer/interface/FitDiagnostics.h"
@@ -477,3 +478,3 @@
 
-	      float d0C2Prob = TMath::Prob(d0DecayVertex->chiSquared(),d0DecayVertex->degreesOfFreedom());
+	      const float d0C2Prob = d0training::vertexProbability(d0DecayVertex->chiSquared(),d0DecayVertex->degreesOfFreedom());
 	      if (d0C2Prob < VtxChiProbCut) continue;
@@ -546,2 +547,4 @@
         sigmaRvtxMag = sqrt(ROOT::Math::Similarity(d0TotalCov, distanceVector2D)) / rVtxMag;
+        const float lVtxSig = d0training::decayLengthSignificance(lVtxMag, sigmaLvtxMag);
+        const float rVtxSig = d0training::decayLengthSignificance(rVtxMag, sigmaRvtxMag);
 
@@ -621,2 +624,3 @@
         theD0->addUserFloat("VtxNdof", d0VtxNdof );
+        theD0->addUserFloat("VtxProb", d0C2Prob);
         theD0->addUserFloat("alpha2D", d0Angle2D );
@@ -628,4 +632,4 @@
         theD0->addUserFloat("decaylength3D", lVtxMag );
-        theD0->addUserFloat("decaylengthsignif2D", rVtxMag/sigmaRvtxMag);
-        theD0->addUserFloat("decaylengthsignif3D", lVtxMag/sigmaLvtxMag );
+        theD0->addUserFloat("decaylengthsignif2D", rVtxSig);
+        theD0->addUserFloat("decaylengthsignif3D", lVtxSig);
         theD0->addUserFloat("dca3D", cur3DIP.value());
@@ -676,4 +680,2 @@
 
-          const float lVtxSig = (sigmaLvtxMag > 0.f ? lVtxMag / sigmaLvtxMag : 0.f);
-          const float rVtxSig = (sigmaRvtxMag > 0.f ? rVtxMag / sigmaRvtxMag : 0.f);
           const float dEta_dau = posCandTotalP.eta() - negCandTotalP.eta();
@@ -686,3 +688,3 @@
             if (name == "y") return theD0->y();
-            if (name == "VtxProb") return d0C2Prob;
+            if (name == "VtxProb") return theD0->userFloat("VtxProb");
             if (name == "3DCosPointingAngle") return std::cos(d0Angle3D);
@@ -692,5 +694,5 @@
             if (name == "3DDecayLength") return lVtxMag;
-            if (name == "3DDecayLengthSignificance") return lVtxSig;
+            if (name == "3DDecayLengthSignificance") return theD0->userFloat("decaylengthsignif3D");
             if (name == "2DDecayLength") return rVtxMag;
-            if (name == "2DDecayLengthSignificance") return rVtxSig;
+            if (name == "2DDecayLengthSignificance") return theD0->userFloat("decaylengthsignif2D");
             if (name == "pTD1") return posCandTotalP.perp();
--- a/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc
+++ b/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc
@@ -1589,3 +1589,6 @@
     if (trk.hasUserFloat("VtxNdof")) ndf[it] = trk.userFloat("VtxNdof");
-    if (vtxChi2[it] > 0.f && ndf[it] > 0.f) VtxProb[it] = TMath::Prob(vtxChi2[it], ndf[it]);
+    if (!twoLayerDecay_ && std::abs(pid_) == kPdgD0)
+      VtxProb[it] = trk.userFloat("VtxProb");
+    else if (vtxChi2[it] > 0.f && ndf[it] > 0.f)
+      VtxProb[it] = TMath::Prob(vtxChi2[it], ndf[it]);
     if (trk.hasUserFloat("alpha3D")) {
@@ -1695,3 +1698,6 @@
         if (d1CC->hasUserFloat("VtxNdof")) grand_ndf[it] = d1CC->userFloat("VtxNdof");
-        if (grand_vtxChi2[it] > 0.f && grand_ndf[it] > 0.f) grand_VtxProb[it] = TMath::Prob(grand_vtxChi2[it], grand_ndf[it]);
+        if (std::abs(d1CC->pdgId()) == kPdgD0)
+          grand_VtxProb[it] = d1CC->userFloat("VtxProb");
+        else if (grand_vtxChi2[it] > 0.f && grand_ndf[it] > 0.f)
+          grand_VtxProb[it] = TMath::Prob(grand_vtxChi2[it], grand_ndf[it]);
         if (d1CC->hasUserFloat("alpha3D")) {
--- /dev/null
+++ b/VertexCompositeProducer/interface/D0TrainingFeatures.h
@@ -0,0 +1,23 @@
+#ifndef VertexCompositeAnalysis_VertexCompositeProducer_D0TrainingFeatures_h
+#define VertexCompositeAnalysis_VertexCompositeProducer_D0TrainingFeatures_h
+
+#include <cmath>
+#include <limits>
+#include <TMath.h>
+
+namespace d0training {
+  inline float decayLengthSignificance(double length, double sigma) {
+    const float missing = std::numeric_limits<float>::quiet_NaN();
+    if (!std::isfinite(length) || !std::isfinite(sigma) || sigma <= 0.) return missing;
+    const float value = length / sigma;
+    return std::isfinite(value) ? value : missing;
+  }
+
+  inline float vertexProbability(double chi2, double ndof) {
+    if (!std::isfinite(chi2) || !std::isfinite(ndof) || chi2 < 0. || ndof <= 0.)
+      return std::numeric_limits<float>::quiet_NaN();
+    return TMath::Prob(chi2, ndof);
+  }
+}
+
+#endif
```

#### D12 체크 완료 및 후속 XY 정의 — 2026-09-30

D12의 AFS 소스 반영과 syntax-only 검증을 완료로 체크했다. 후속 사용자 지시로 XY track error 7곳은 기존 CmsHI D03 reference에 맞췄고 네 소스 문법 검사를 통과했다. D12 header와 공통 userFloat 경로는 현재 소스에서도 유지된다. 두 변경의 library build/event 검증은 미실행이다. [XY 변경 보고서](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/xy_dca_CmsHI_reference_restore_20260930/REPORT.md) · [현재 소스 검증](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/xy_dca_CmsHI_reference_restore_20260930/verification.json)

#### 정확히 확인한 모델·인터페이스

- AFS ONNX: 547,090 bytes, SHA256 `cd431de656a401610062481e5b6a88f20b534231836f631e9cf49d2461bbc024`.
- D0Producer::initializeGlobalCache의 FileInPath는 `VertexCompositeAnalysis/VertexCompositeProducer/data/<configured filename>`이다.
- ONNX float_input: float tensor [batch,20]. probabilities: [batch,2], class labels [0,1]. 현재 출력 index 1은 signal class 1의 확률과 맞는다.
- Native 모델: 20 ordered feature names, saved 561 trees, best_iteration=510. 이번 native/ONNX 비교는 모든 저장 tree를 사용하며 511-tree prefix 모델과 혼동하지 않는다.
- 원래 학습 source ROOT/probe provenance의 파일 size·mtime와 native JSON SHA256을 대조했다. 보존 실제-row probes를 사용했으며 1,385,095,814-byte 학습 ROOT 전체 hash를 새로 계산하거나 전체 sample을 재구성한 검사는 아니다.

#### 20개 입력 대조표 (0-based)

|index|feature|학습 branch/파생값 정의|현재 D0Fitter 값|단위|판정|
|---|---|---|---|---|---|
|0|pT|D0 parent pT; same stored candidate pT|theD0->pt()|GeV/c|정상 값 정의 일치|
|1|y|D0 parent rapidity|theD0->y()|1|정상 값 정의 일치|
|2|centrality|stored centrality bin, not percent|static_cast<float>(centrality), centralityBin:HFtowers|hiBin 0–199|척도 일치; 과거 training flat의 event 연결은 별도 검증 범위|
|3|VtxProb|stored vertex chi2 probability|d0training::vertexProbability → stored VtxProb userFloat|1|유효 chi2/ndf에서 정의 일치; D12 예외 정책 AFS 소스 반영|
|4|3DCosPointingAngle|cos of stored 3D pointing angle|cos(d0Angle3D)|1|정상 값 정의 일치|
|5|3DPointingAngle|stored 3D pointing angle|angle(PV-to-SV, D0 momentum)|rad|정상 값 정의 일치|
|6|2DCosPointingAngle|cos of stored 2D pointing angle|cos(d0Angle2D)|1|정상 값 정의 일치|
|7|2DPointingAngle|stored 2D pointing angle|angle(transverse PV-to-SV, D0 transverse momentum)|rad|정상 값 정의 일치|
|8|3DDecayLength|stored 3D decay length|lVtxMag = \|SV - reference vertex\||cm|정상 값 정의 일치|
|9|3DDecayLengthSignificance|stored 3D length/sigma, NaNs preserved by loader|d0training::decayLengthSignificance → shared decaylengthsignif3D userFloat|1|양의 유효 sigma에서 일치; D12 무효값 NaN 정책 AFS 소스 반영|
|10|2DDecayLength|stored 2D decay length|rVtxMag = transverse \|SV - reference vertex\||cm|정상 값 정의 일치|
|11|2DDecayLengthSignificance|stored 2D length/sigma, NaNs preserved by loader|d0training::decayLengthSignificance → shared decaylengthsignif2D userFloat|1|양의 유효 sigma에서 일치; D12 무효값 NaN 정책 AFS 소스 반영|
|12|pTD1|stored daughter 1 candidate pT|posCandTotalP.perp(), positive-charge fitted daughter|GeV/c|정상 값 정의 일치; K/pi order is not fixed|
|13|EtaD1|stored daughter 1 candidate eta|posCandTotalP.eta()|1|정상 값 정의 일치|
|14|pTerrD1|stored original daughter 1 absolute track ptError; no pT division|positiveTrackRef->ptError()|GeV/c|절대 오차 정의 일치|
|15|pTD2|stored daughter 2 candidate pT|negCandTotalP.perp(), negative-charge fitted daughter|GeV/c|정상 값 정의 일치; K/pi order is not fixed|
|16|EtaD2|stored daughter 2 candidate eta|negCandTotalP.eta()|1|정상 값 정의 일치|
|17|pTerrD2|stored original daughter 2 absolute track ptError; no pT division|negativeTrackRef->ptError()|GeV/c|절대 오차 정의 일치|
|18|Trk3DDCA|stored track3DDCA of the two original daughter tracks|TwoTrackMinimumDistance.distance(), stored userFloat track3DDCA|cm|정상 값 정의 일치; D0-to-PV DCA is a different field|
|19|dEta_dau|loader derives EtaD1 - EtaD2 with sign kept|posCandTotalP.eta() - negCandTotalP.eta()|1|정의와 부호 일치|

D0Fitter에서 fit daughter momenta를 pos/neg 순서로 만들고 동일 순서로 RecoChargedCandidate daughter를 저장한다. PAT6는 parent pT/y, daughter pT/eta, original TrackRef::ptError 및 D0 userFloat의 angle/length/significance/track3DDCA를 읽는다. simpleDMC copy path는 이 branch들을 복사하며, loader는 mass·matching·MC label 등을 feature에서 제거한 후 signed dEta를 추가한다. pTerr feature 자체와 track 사전 cut의 상대 ptError/pT는 별개다. VtxProb 예외 표현과 chi2=0의 확률 정책도 기존 D12대로 AFS 소스에 반영됐다. FIT-02의 covariance 유지 결정 및 fit 결과의 일반적인 χ²/ndf/finite 검사 범위와 구분한다.

#### 이식 전 significance 예외 입력 차이 측정

MVA 입력을 대조한 소스 snapshot의 D0Fitter.cc:675–676:

```cpp
const float lVtxSig = (sigmaLvtxMag > 0.f ? lVtxMag / sigmaLvtxMag : 0.f);
const float rVtxSig = (sigmaRvtxMag > 0.f ? rVtxMag / sigmaRvtxMag : 0.f);
```

이식 전 조사 snapshot의 저장 userFloat는 :627–628에서 `rVtxMag/sigmaRvtxMag`, `lVtxMag/sigmaLvtxMag`를 조건 없이 저장한다. PAT6 저장 branch와 학습 loader는 이를 그대로 사용하며 loader에 NaN→0 치환은 없다. sigma가 NaN이면 `sigma>0` 비교는 false라서 ONNX 입력은 0이지만 저장 significance는 NaN이다. 유효한 양의 sigma에서는 같은 식이다. sigma=0/nonpositive의 처리도 식이 다르며 실제 발생률은 이번에 측정하지 않았다.

실제 학습 artifact에서 보존된 significance NaN 후보 1,036개(1,745 NaN cells)의 column 9/11 NaN만 0으로 바꿔 현재 AFS ONNX의 CPU 점수를 비교했다. 분모 sigma는 flat에 없으므로 각 NaN의 발생 원인을 특정한 검사가 아니다. 생산 event 처리나 최종 물리 bias 측정도 아니다.

|표본|NaN 후보 수|MVA>0.9, NaN 유지|MVA>0.9, NaN→0|fail→pass|
|---|---|---|---|---|
|signal|32|27|29|2|
|background|1,004|4|33|29|

같은 값들을 native 모델과 현재 ONNX에 넣었을 때 finite 10,000, NaN 1,036, zero 1,036 행에서 최대 score 차이는 1.267e-6 이하이고 MVA>0.9 판정 차이는 0건이었다. 이는 같은 입력에 대한 모델 출력 일치다. 두 서로 다른 NaN/zero 입력은 위 표처럼 다른 선택을 만든다. ONNX Runtime 1.21.0/XGBoost 2.1.1/NumPy 1.26.4 CPU 검사이며 CMSSW ONNXRuntime 1.14.1 event 실행을 새로 수행한 것은 아니다.

#### 기존 EOS D12 결정과 AFS 반영 상태

2026-09-30 소스 이식 전에 준석이 지적한 기존 integrity 기록을 확인했다. [D12 체크리스트](/eos/user/j/junseok/DstarFitterIntegrity_20260909/test/decision_checklist_20260910/CHECKLIST.md:90)와 [2026-09-10 구현·검증 보고서](/eos/user/j/junseok/DstarFitterIntegrity_20260909/test/d0_feature_consistency_20260910/REPORT.md:3)에 사용자 승인과 완료 상태가 기록돼 있다. 예외 처리 정책을 새로 결정할 항목으로 표시한 이전 요약을 정정한다.

|항목|이미 결정한 D12 정책|
|---|---|
|2D/3D decay-length significance|L와 sigma가 유한하고 sigma>0이며 float 결과도 유한할 때 L/sigma, 그 외 NaN|
|D0 VtxProb|유한한 chi2>=0 및 ndof>0에서 TMath::Prob; 유효한 chi2=0이면 1, 그 외 NaN|
|저장·추론 공통값|D0Fitter에서 한 번 계산해 userFloat에 저장하고 PAT6와 ONNX가 같은 값을 읽음. D* 안의 D0 VtxProb도 동일 저장값 사용|

현재 구현은 [D0TrainingFeatures.h](/eos/user/j/junseok/analysis/dstarana/VertexCompositeStudies/source_variants/fitter_integrity/VertexCompositeAnalysis/VertexCompositeProducer/interface/D0TrainingFeatures.h:9)에 보존돼 있다. `/eos/user/j/junseok/analysis/dstarana/VertexCompositeStudies/source_variants/fitter_integrity/VertexCompositeAnalysis`의 D0Fitter는 이 함수를 쓰고 ONNX가 `decaylengthsignif3D/2D` 및 `VtxProb` userFloat를 읽는다. Header SHA256은 당시 `current_sha256.json`과 정확히 일치한다. Legacy alias 아래 `VertexCompositeAnalysis`는 원본 snapshot이며 D12 source variant와 구분해야 한다.

이식 직전 AFS에는 이 header가 없고 D0Fitter는 sigma 조건이 false일 때 0을 전달했다. 아래 새 이식 기록에서 기존 D12 정책을 AFS에 반영했다. 앞선 기록 정정 단계에서는 source 변경·build·cmsRun·재학습을 수행하지 않았으며, 이번 승인된 단계에서는 D12 소스 변경과 syntax-only 검증을 수행했다. Build·cmsRun·재학습은 수행하지 않았다.

기존 검증 산출물: 경계조건 18개, MC 100 입력 events/1,069 D0 후보 및 data 200 입력 events/3,083 D0 후보, D* MC 100 입력 events/195 후보 및 data 500 입력 events/4 후보. 당시 배포 ONNX hash는 현재 AFS 모델과 같다. `deployed_onnx.json` 원문에서 9개 입력의 Python/CMSSW 최대 확률 차이는 1.1920929e-07; 모두 유한했다. 이는 보존한 당시 검증이며 현재 AFS 수정 후 runtime 검증으로 표시하지 않는다. 상세는 [당시 validation](/eos/user/j/junseok/DstarFitterIntegrity_20260909/test/d0_feature_consistency_20260910/validation/deployed_onnx.json)와 [이번 결정·경로 대조](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/mva01_feature_definition_check_20260930/prior_D12_reconciliation_20260930/reconciliation.json)에 기록했다.

#### centrality와 과거 provenance 범위

현재 D0Fitter는 해당 event의 centralityBin:HFtowers integer를 사용한다. 학습 feature도 percent로 나누지 않은 0–199 hiBin이다. 의미·척도는 일치한다.

보존된 2026-09-10 mixing audit에는 candidate tree와 EventInfo의 동일 entry를 같은 event로 사용하던 경로와 PV 연결에 근거한 centrality 불일치 관측이 있다. 현재 FlatMixWithPromptNP.cpp는 candidate tree의 centrality branch를 직접 읽도록 변경돼 있으므로 그 과거 문제를 현재 mixing 코드의 미수정 결함으로 표시하지 않는다. 이번에 검사한 March 학습 ROOT/native 모델은 과거 artifact이며 현재 mixing 코드의 수정이 이 모델의 학습 입력을 소급 수정한 것은 아니다. 보존 과거 보고서와 집계는 첨부하되 기존 PV 기반 관측을 원래 EDM event-key join 확정 증거로 확대하지 않는다. 이번에 historical flat을 재생성하거나 이 영향의 점수/physics bias를 측정하지 않았다.

04Mar26 PR/NP pT1 CRAB PSetDump에서 d0ana_newreduced=PATCompositeTreeProducer6, generalD0CandidatesNew:D0 및 양/음 daughter 구조를 확인했다. Archived PSet는 확인했지만 job-time 전체 source/library snapshot과 March training/export의 불변 manifest까지 입증한 것은 아니다. 현재 소스와 보존 소스/ROOT/모델의 증거 수준을 구분한다.

#### 남은 구현과 provenance 판단

1. 기존 D12의 AFS 소스 반영과 syntax-only 검증은 완료했다. 수정 라이브러리 build 및 event 검증은 아직 수행하지 않았다.
2. 과거 학습 flat centrality 연결 관측이 실제 모델에 미치는 영향을 현재 작업 범위에서 다룰 필요가 있는지. 새 mixing/학습은 수행하지 않았다.

#### 증거

- [20 definitions and source hashes](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/mva01_feature_definition_check_20260930/feature_definition_check.json)
- [live cfg/source evidence](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/mva01_feature_definition_check_20260930/configuration_and_source.json)
- [deployed ONNX and measured probe results](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/mva01_feature_definition_check_20260930/model_and_probe_check.json)
- [March CRAB archived modules](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/mva01_feature_definition_check_20260930/training_crab_archive.json)
- [preserved historical source evidence](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/mva01_feature_definition_check_20260930/historical_source_evidence.txt)
- [preserved historical mixing report](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/mva01_feature_definition_check_20260930/historical_mixing_report.md)

### NT-04/NT-05 확인 — 2026-09-30

**NT-04:** 대표 data 원래 event tree의 `candSize`를 읽었다. `newBDT_0311/Data` 아래 Prime0–7, 8–15, 16–23, 24–31의 4개 CRAB task에서 첫/중간/마지막 chunk의 중간 filename을 선택해 총 12파일의 모든 저장 entry를 검사했다. 별도 pT/y/centrality/MVA cut을 적용하지 않았다. 총 194,677 events, D* 최대 262개, D0 최대 36개, `candSize>=50,000`은 두 tree 모두 0건. 전체 원본 production 전수 검사는 아니지만 준석의 현재 상한 유지 판단을 뒷받침한다. Cap에 의해 후보 집합이 달라지는 문제의 추가 조사는 종료한다. 다른 입력 label이나 fit 변경에 따른 후보 집합 차이는 이 결과로 검증한 것이 아니다.

**NT-05 현재 수정 상태:** 2026-09-30 준석 승인으로 event scalar 갱신 호출 3개를 invalid CCC return 앞으로 이동했다. Source diff와 GCC syntax-only 검증은 완료했다. 아래 2-event 값은 수정 전 기존 library의 버그 재현 증거이며 수정 후 runtime 성공으로 표시하지 않는다. 수정 library build/event 재검증은 아직 하지 않았다. [diff](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/nt05_scalar_update_20260930/nt05_scalar_update.diff), [verification](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/nt05_scalar_update_20260930/verification.json).

**NT-05 이전 검증 수준:** 기존 audit의 missing collection 문구는 source 검토에서 나온 잠재 경로였으며, missing collection을 넣은 event 실행 결과는 없었다.

**이번 runtime 재현:** 기존 CMSSW_13_2_11 library로 `pr4_miniaod.root`의 첫 두 events를 처리했다. 높은 track-pT cut으로 fit을 실행하지 않는 D0Producer가 정상 empty CCC를 생성하게 하고, 시험용 복제 producer는 첫 event에서만 실행했다. 대조군 PAT6는 두 events 모두 정상 empty CCC를 읽고 시험군은 두 번째 event에서 CCC handle이 invalid였다. 두 분석기는 동일한 unpacked PV/tracks를 읽는다. 두 번째 event에서 invalid collection warning 뒤에도 시험군 tree가 Fill됐다.

|값|event1 공통|event2 정상 empty CCC|event2 missing CCC|
|---|---|---|---|
|EventNb|320503557|320503293|320503293|
|candSize|0|0|0|
|PV x (cm)|0.037988309|0.037984401|0.037988309|
|PV y (cm)|−0.017846068|−0.017579934|−0.017846068|
|PV z (cm)|4.406503201|3.236734867|4.406503201|

**확인된 결론:** candidate가 0개인 정상 collection과 collection 자체의 누락은 다르다. 정상 empty CCC는 PV 정보를 갱신했지만 missing CCC에서는 x/y/z가 이전 event 값으로 남았다. Event key는 갱신되므로 event 번호까지 재사용된 것은 아니다. 이 테스트에서 Ntrkoffline은 양쪽 두 events 모두 0이어서 track 수의 재사용 여부는 실측 판정하지 않았다. Centrality/EP 기능은 껐으므로 그 scalar들의 runtime 재사용도 이번 측정으로 판정하지 않는다. 정상 production에서 collection 누락이 실제 발생했다는 증거를 얻은 것은 아니다.

Library는 기존 official SHA256 `cec3105f5c1b5add5a26da89b5ef1a1c085dbc17ae3ae91330063c4be69bfce9`를 사용했고 rebuild하지 않았다. 현재 소스의 EP-01/FIT-05 수정 runtime 검증으로 취급하지 않는다. Test cfg와 source/library hashes, 실행 log, control/시험군 측정은 아래 evidence에 기록했다. Production source/config는 변경하지 않았다.

- [NT-04 files/counts](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/nt04_nt05_check_20260930/nt04_candidate_cap.json)
- [NT-05 measured scalars and checks](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/nt04_nt05_check_20260930/nt05_runtime_result.json)
- [NT-05 cmsRun log](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/nt04_nt05_check_20260930/nt05_runtime.log)
- [NT-05 runtime provenance](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/nt04_nt05_check_20260930/runtime_provenance.json)

### FIT-06 설명: 현재 저장 방식 유지·추가 진단 저장 미채택

**현재 결정 — 준석의 후속 지시 반영:** 현재 작업에서 실패/탈락 pair의 별도 진단 저장을 도입하지 않고 기존 저장 방식을 유지한다. 기존 cut·fit 실패 처리·성공 후보 저장은 변경하지 않는다. 이는 추가 저장 필요성에 대한 사용자 결정이며 영향이 작다는 새로운 측정 결과를 얻었다는 뜻은 아니다. 아래는 기존 코드 사실과 당시 제안의 기록이다.

**확인한 코드 사실:** 현재 DStar 단계의 pair는 이미 만들어진 D0 collection과 선택된 slow-track 목록의 조합이다. 이 단계 전에 탈락한 D0/track 또는 event의 전체 효율까지 포함하는 분모는 아니다.

1. [raw Δm gate](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:491)에서 `Mraw(D0+slow)−Mraw(D0)>0.160 GeV`이면 fit을 시도하기 전에 `continue`한다.
2. [내부 D0 fit](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:587), [DStar fit](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:604)의 invalid tree/state/vertex도 `continue`로 탈락한다.
3. 후속 pT/y/vertex/mass 조건을 통과한 후보만 [기존 candidate collection](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:816)에 저장된다. 현재 debug cutflow/count는 단계별 집계이며 탈락 pair의 입력·결과를 후보별로 보존한 출력이 아니다.

**해석:** 성공 후보에 before/after mass branch를 추가하면 그 후보들의 변화는 볼 수 있다. fit 실패 pair는 저장 출력에 없으므로 fit 실패율의 분모가 복원되지 않는다. raw gate 밖 pair가 refit 후 분석 구간 안으로 들어오는지도 현재 저장 후보만으로 판단할 수 없다.

**과거 진단 설계 제안 — FIT-06 유지 결정으로 미채택:** nominal candidate selection을 유지하면서 pair identity, raw Δm, fit 시도 여부, 단계별 실패/탈락 이유, 성공한 경우 refit Δm와 각 cut 결과를 별도 진단 출력에 보존하는 범위를 정해야 한다. Fit 미시도와 fit 실패를 구분하고 각 fit의 attempted-pair 분모를 따로 둔다. prefit 밖→refit 안 유입을 직접 측정하려면 진단용의 더 넓은 pair 범위에서 fit을 시도해야 하며, raw 정보만 저장해서 그 refit 결과를 추정할 수는 없다. 이는 기존 prefit cut을 nominal에서 제거하기로 결정했다는 뜻이 아니다.

공식 [CMS Kinematic Vertex Fit guide, §The sequential fit](https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideKinematicVertexFit#The_sequential_fit)의 표현은 fitter가 “returns the consistent decay tree”라는 것이다. 실패 pair 미보존이라는 위 결론은 현재 producer의 `continue`와 저장 위치를 직접 읽은 결과다.

### FIT-07 설명: 조사 완료·현재 수렴/seed 기본 설정 유지

- **수렴 기준:** 반복 중 vertex 이동이 정해진 거리보다 작으면 종료하는 조건과 최대 반복 횟수. 현재 기본값은 XY 100 μm와 100회다. [공식 CMSSW_13_2_11 fitter](https://github.com/cms-sw/cmssw/blob/CMSSW_13_2_11/RecoVertex/KinematicFit/src/KinematicParticleVertexFitter.cc#L39-L44)의 `maxDistance=0.01` cm, `maxNbrOfIterations=100`을 확인했다.
- **Seed:** 반복을 시작하는 vertex 위치. 서로 다른 시작점에서 같은 입력이 같은 결과로 수렴하는지 검사하는 항목이다. 이 seed의 loose covariance(대각 10000 cm²)는 측정된 PV covariance를 제약으로 넣은 것이 아니다.
- **이미 수행한 범위:** [stability 보고서](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/fit_stability_20260929/REPORT.md)의 1,264후보(PR 716, NPR 548), 15설정=18,960 fit 호출. Reco pT 7–10 GeV, abs(y)<0.3, centrality 0–10%, **tracker-axis** abs(cosθ*) 0.6–0.8, 원래 MVA>0; pass는 MVA≥0.95. 기존 후보·weight·MVA·cos 구간을 고정했고 누락 NPR 17개는 포함되지 않았다. 모든 stream 또는 전체 phase space의 검사가 아니다.

중앙 비중은 `sum(w·Ibin)/sum(w)`의 **가중 비율**, 68% 폭은 가중 `Q84−Q16`이다. 중앙 bin은 fitted-children Δm의 `[145.28813559322035,145.5084745762712)` MeV. 유입/유출은 paired 후보 개수다. 실제 계산은 [physics_checks.py](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/fit_stability_20260929/physics_checks.py)와 [mass_response.csv](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/fit_stability_20260929/mass_response.csv)에서 대조했다.

|확인한 결과|의미|
|---|---|
|최대 반복만 100→300: 1,264개 모두 Δm 변화 0|이 표본에서 반복 한도 부족이 주된 설명이 아님|
|XYZ 1 μm: PR pass 491개의 가중 중앙 비중 19.791067%→19.791067%; 중앙 유입/유출 0/0|수렴 강화만으로 이 표본의 중앙 집중이 해결되지 않음|
|같은 PR pass의 가중 68% 폭 1.448572→1.448741 MeV|이 변경에서 질량 폭의 실질적인 개선은 관측되지 않음|
|일부 seed에서 큰 vertex/Δm 이동 또는 실패; 대표 9209는 valid이나 χ²=−12.734|valid 결과와 수치 안정성/물리적 신뢰성은 같은 판정이 아님|

**현재 결정 — 준석의 후속 지시 반영:** 이미 수행한 비교에서 수렴 기준 강화로 중앙 집중이 개선되지 않았으므로 **현재 수렴 기준과 기본 vertex seed를 유지하고 FIT-07의 추가 튜닝 검토를 종료한다.** 이 항목을 미확정 수정 목록에서 제외한다. 표본에서 관측한 조기 종료·seed 민감·음수 χ² 예외와 원래 stability 보고서는 완료된 조사 기록으로 보존한다. 현재 기본 설정 유지가 관측된 예외의 삭제나 전체 phase space 정상성 검증을 뜻하지는 않는다. 전체 pair의 실패율/후보 유입·유출은 이 표본 조사로 측정한 값이 아니며, 관련 FIT-06도 현재 저장 방식 유지로 결정했다.

## 질문한 EventInfo 세 항목의 의미

### EV-01: 첫 HLT 기록이 실제 trigger 결과를 반영하지 못하는 경로

설정은 `HLT_HIMinimumBiasHF1AND_v*`, 실제 HLT 이름은 예를 들어 `HLT_HIMinimumBiasHF1AND_v7`이다. EventInfo 코드의 `std::string::find()`는 `*`를 wildcard로 처리하지 않고 문자 그대로 찾는다. 따라서 해당 경로를 찾지 못하고 초기값 `trigHLT[0]=false`, `trigPrescale[0]=-9`가 남는다.

이는 **EventInfo 기록의 문제**다. 별도의 hltHighLevel event filter는 자체 pattern 처리로 동작하므로 이 설명만으로 사건 선택 전체가 실패했다는 뜻은 아니다. EventInfo 설정의 첫 항목을 다른 두 항목처럼 `_v` prefix로 만드는 것이 현재 substring 구현에 맞는 후보 수정안이다. 실제 HLT filter의 `_v*`까지 일괄 변경하는 수정안은 아니다. Fix02에서 data Step2 두 설정에 반영했다. 상세 범위는 아래 Fix02 이력 참조.

### EV-02: HF filter 실패와 미실행을 구분할 수 없는 기록

`eventFilterNames`에는 `Flag_hfCoincFilter`가 있지만 해당 Path와 schedule 항목은 주석 처리돼 있다. EventInfo는 각 `evtSel` 슬롯을 false로 초기화하고, TriggerResults에 그 path가 있을 때만 실행/통과 여부를 채운다. 수정 전 설정에서는 `evtSel[1]=false`가 남는다. 현재 data/MC 4개 진입 설정에서는 해당 flag path를 실행하도록 바뀌었다. 이것을 “실제로 HF filter를 실행했는데 떨어졌다”로 해석하면 안 된다.

HF filter를 **기록만 할지**, DStar event selection의 **필수 cut으로 적용할지**, 사용하지 않는 기록 항목을 **제거할지**는 다른 결정이다. 단순히 주석을 풀면서 물리적 event selection까지 바꿔서는 안 된다. Fix02에서는 세 filter를 data DStar path의 필수 cut으로 적용하고 독립 flag도 기록하도록 수정했다.

### EV-03: 기존 32-bit EventNb 유지 결정 — 2026-09-30

**최종 결정:** 준석의 지시대로 PAT6/custom EP/EventInfo의 기존 unsigned 32-bit `EventNb`와 ROOT `/i` 저장을 유지한다. 이번 판단에서 full-width branch 추가나 기존 branch의 64-bit 교체를 채택하지 않았다.

판단에 사용한 확인 자료:

1. EDM event 번호 자체는 `unsigned long long`이다. 현재 ntuple의 보존 한계는 **4,294,967,295**이며, 저장 폭 차이가 존재한다는 소스 사실은 유지한다.
2. 다른 공개 VertexCompositeAnalysis에서도 같은 32-bit 저장을 확인했다: [davidlw ParticleFitter_13_2_X의 EventInfoTreeProducer](https://github.com/davidlw/VertexCompositeAnalysis/blob/5060ad46272152003940428bc2fe94971fcb14d1/VertexCompositeAnalyzer/plugins/EventInfoTreeProducer.cc#L353), [10_3_X의 PATCompositeTreeProducer](https://github.com/davidlw/VertexCompositeAnalysis/blob/e8744b55fae9dc594dcf709c3d8754d0a8badd5b/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer.cc#L1301). 같은 13_2_X의 ParticleAnalyzer도 UInt_t를 사용하지만 `getUInt`에서 범위를 검사한다는 구현 차이는 있다.
3. 원본 MiniAOD `EventAuxiliary`를 선택 조건 없이 읽은 [실측 결과](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/ev03_miniaod_event_numbers_20260930/scan.json): Run 375823의 Prime0 106파일·8,976,271 events와 Run 375064 group-copy 1파일·88,888 events, 합계 **107파일·9,065,159 events**에서 **32-bit 최대값 초과 0**. 전체 최대 event 번호는 **870,945,840**. 각 파일의 모든 entry를 읽었고 manifest와 결과 파일 목록이 일치함을 확인했다.
4. 이는 **32개 HIPhysicsRawPrime stream 전체 검사 결과가 아니다**. 당시 CMS DAS 규모 조회는 v2 전체 181,630파일·15,928,759,266 events, v1+v2 194,340파일·19,737,110,423 events였다. 다른 코드가 32-bit를 쓴다는 사실과 제한된 표본에서 overflow가 없다는 사실을 전체 DATA의 overflow 부재 증명으로 확대하지 않는다.
5. 저장 비용 비교도 설명했다: 기존 32→64 교체는 tree entry당 +4바이트, 기존 branch 유지+64 추가는 +8바이트(압축 전). 실제 ROOT 압축 후 증가량은 측정하지 않았다. **최종적으로 준석이 기존 저장 형식 유지를 지시했다.**

DAS/DBS의 `nevents`는 event 개수이며 최대 event 번호가 아니다. MiniAOD의 `IndexIntoFile` event-number 목록은 transient로 저장되지 않고, 실제 파일 헤더에도 최대 event 번호 요약이 없는 것을 [메타데이터 점검 로그](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/ev03_miniaod_event_numbers_20260930/metadata_inspection.log)와 CMSSW 저장 정의로 확인했다. 따라서 32 stream 전수 검사는 메타데이터 조회만으로 완료했다고 기록하지 않는다.

이 결정에 따라 아래 과거 44branch 제안의 `diagEvent64` 추가는 채택하지 않는다. 과거 조사 원문과 명세는 당시 제안의 기록으로 보존한다.

## EP-01 수정 이력

- 수정 파일: [PATCompositeTreeProducer6.cc:526](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc:526).
- 함수: `PATCompositeTreeProducer6::processEventPlaneInfo`.
- 근거: CMSSW_13_2_11 `HiEvtPlaneList.h`: trackm2/3는 index 4/10, η ∈ (−2,−1); trackp2/3는 index 5/11, η ∈ (1,2).
- 수정: `eptrackp*` 4/10→5/11, `eptrackm*` 5/11→4/10. `[0]`=n2, `[1]`=n3.
- 적용 필드 10종: `Angle`, `Angleoff`, `AngleRaw`, `Q`, `SumW`, `SumCosRaw`, `SumSinRaw`, `SumCos`, `SumSin`, `SumPtOrEt`. 두 방향×두 harmonic×10종 = **40개 대입**.
- [작업 직전→직후 전체 source diff](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix01_tracker_ep_pm/tracker_ep_pm.diff).
- [검증 기록](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix01_tracker_ep_pm/verification.json).
- 소스 수정만 완료. Build/cmsRun/production 제출은 수행하지 않았으며, 기존 ROOT와 라이브러리는 갱신하지 않았다.
- 기존 ROOT는 과거 반대 의미를 유지한다. 수정 후 빌드·생산한 ROOT와 비교할 때 p/m별 calibration 및 subevent 정의가 같은 물리적 η 구간을 가리키는지 맞춰야 한다. 이전 파일의 내용을 자동으로 수정한 것은 아니다.

```diff
-  eptrackpAngle[0] = (hasEP ? (*eventplanes)[4].angle(2) : kInvalid);
-  eptrackpAngle[1] = (hasEP ? (*eventplanes)[10].angle(2) : kInvalid);
-  eptrackmAngle[0] = (hasEP ? (*eventplanes)[5].angle(2) : kInvalid);
-  eptrackmAngle[1] = (hasEP ? (*eventplanes)[11].angle(2) : kInvalid);
+  eptrackpAngle[0] = (hasEP ? (*eventplanes)[5].angle(2) : kInvalid);
+  eptrackpAngle[1] = (hasEP ? (*eventplanes)[11].angle(2) : kInvalid);
+  eptrackmAngle[0] = (hasEP ? (*eventplanes)[4].angle(2) : kInvalid);
+  eptrackmAngle[1] = (hasEP ? (*eventplanes)[10].angle(2) : kInvalid);
```

위는 flat angle의 발췌 diff다. 나머지 36개 대입을 포함한 전체 변경은 링크의 diff에 있다.

[수정 전 전체 감사 보고서](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/REPORT.md)


## Fix02: 2023 PbPb data offline event selection — 2026-09-29

이 이력이 앞선 EV-01/02/04의 수정 전 설명을 갱신한다. 수정 전 상태와 현재 상태를 구분한다.

- 공식 권고: 2023 PbPb는 HLT + primary vertex + cluster compatibility + HF 2Th4. Beam-scraping은 이 2023 PbPb 권고 목록에 없다. 2024는 HF 3Th5이므로 연도를 섞지 않는다.
- [CMS 권고](https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideHeavyIonCentrality#Event_selection).
- Fix02 당시 대상은 data Step2 Condor cfg 및 canonical/CRAB PSet cfg 두 파일이었다. **MC는 이후 Fix03에서 두 진입 설정에 반영됐으며, 현재 MC 미수정이라는 뜻이 아니다.** 보관된 CRAB 입력 snapshot과 Step1은 이 변경 대상이 아니다. MC GEN 분모 범위는 아래 Fix03 참조.
- `colEvtSel`에 primaryVertexFilter, clusterCompatibilityFilter, phfCoincFilter2Th4를 연결하고, DStar analysis path에서 D0 producer보다 앞에 배치했다.
- HF는 MiniAOD의 `hiHFfilters:hiHFfilters`를 읽는 HiHFFilter(threshold=4, minnumtowers=2)를 사용한다. 옛 particleFlow 재계산 sequence의 import를 제거했다.
- PV는 CmsHI 13_2_X MiniAOD 공식 정의와 일치하도록 `offlineSlimmedPrimaryVertices`, `!isFake && abs(z) <= 25 && position.Rho <= 2`를 지정했다. Slimmed vertex에는 TrackRef가 남지 않으므로 그 collection에 tracksSize>=2를 요구하지 않는다. 기존 fitter 자체의 PV 선택을 바꾼 것은 아니다. 분석의 추가 |vz|<15 cut과 이 표준 filter를 혼동하지 않는다.
- Cluster compatibility 설정은 공식 cfi와 byte-identical임을 확인했고 parameter 변경은 없다.
- 독립 flag들을 실제 schedule에 추가했다. **evtSel 길이 3→4**, 순서 `[Flag_colEvtSel, Flag_hfCoincFilter, Flag_primaryVertexFilter, Flag_clusterCompatibilityFilter]`. 인덱스 2는 과거 PV+cluster 결합 의미에서 **PV 단독**으로 정리했다. 인덱스 0은 HLT+세 offline filter 모두 통과한 결과다.
- EventInfo만 첫 HLT pattern `_v*`→`_v`로 수정했다. 실제 hltHighLevel의 wildcard는 유지한다.
- EventInfo는 기존처럼 dStarAna_step 통과 event를 기록하므로 실패 event를 모아 새 효율 분모를 만드는 변경은 아니다. Event-selection 효율/손실률은 별도 event-level cutflow로 측정해야 한다.

- [이번 두 cfg 전체 diff](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/production_offline_selection.diff)
- [검증과 설정별 경로 보고서](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/REPORT.md)
- [Condor CMSSW 검증](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/condor_validation.json) / [canonical CMSSW 검증](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/canonical_validation.json)
- 검증: CMSSW_13_2_11에서 두 실제 cfg를 load, MiniAOD 변환 이후 source/threshold/순서/개별 flag/HLT pattern 검사 통과. Python syntax 및 patch 검사 통과. Build/cmsRun event 처리/production 제출은 하지 않았다.
- 기존 data ROOT는 이 수정으로 변경되지 않는다. Event-selection cut 적용은 수정된 설정을 사용한 새 production부터다.


## Fix03: MC 설정 반영 상태 — 현재 파일 및 기존 검증 재대조

이 절은 누락됐던 MC 수정 이력을 보완한다. 아래 두 현재 파일은 보존된 Fix03 직전 파일 + 수정 diff와 정확히 일치한다. 이번 문서 보완에서 MC cfg나 C++를 추가 수정한 것은 아니다.

- [PbPb2023_D0BothAndDStar_MB_cfg_mc_v2_Step2MVA.py](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/production/pbpb2023/mc/PbPb2023_D0BothAndDStar_MB_cfg_mc_v2_Step2MVA.py)
- [PbPb2023_D0BothAndDStar_MB_cfg_mc_Step2MVA_Condor_v1.py](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_mc_Step2MVA_Condor_v1.py)

- Data와 같이 DStar path의 D0 producer 앞에 PV·cluster compatibility·HF 2Th4를 적용했다.
- EventInfo 첫 HLT 기록 `_v*`→`_v`, evtSel 순서 `[전체, HF, PV, cluster]` 4칸, 개별 flag path와 schedule도 반영됐다. 실제 HLT filter wildcard는 유지한다.
- 기존 [MC 수정 diff](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix03_mc_offline_selection/mc_offline_selection.diff), [canonical 설정 검증](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix03_mc_offline_selection/canonical_validation.json), [Condor 설정 검증](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix03_mc_offline_selection/condor_validation.json)을 대조했다. 두 기록 모두 `passed=true`, **`runtime_events_processed=0`**이다. 설정에 들어간 상태이지 실제 MC event 처리나 전체 production 반영 완료를 뜻하지 않는다.
- 해당 두 PSet에서 `genDstarEventPlaneMiniAOD`도 offline filter 뒤의 같은 DStar path에 있다. 이 경로의 GEN 기록을 무조건적인 전체 생성 event 분모로 취급하면 안 된다. 이 문서 보완에서는 GEN 분모 경로나 selection을 새로 바꾸지 않았다.
- 범위는 위 MC Step2 두 파일이다. 다른 MC PSet, 이미 제출된 CRAB job snapshot, 과거 ROOT까지 모두 바뀌었다고 주장하지 않는다.
- 현재 소스/diff/hash 대조: [verification.json](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/document_reconciliation/verification.json).

## FIT-05: AFS 원본 코드·옵션과 Fix04/05

[AFS DStarFitter.cc:469](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:469)의 Fix04 직전 파일은 git HEAD와 byte-identical이었다. 해당 로직은 **2026-07-27 커밋 `74b53a3`**에 이미 들어 있었으며 이번 EOS 진단이나 event-selection 수정에서 만든 로직이 아니다. [git blame 기록](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/document_reconciliation/FIT05_blame.txt).

수정 직전 로직:

```cpp
bool duplicateSlowPion = false;
if (debugCategoryCutflow_ && debugCat >= 0) {
    // D0 daughter TrackRef와 slow-pion TrackRef 비교
    // 같으면 duplicateSlowPion = true;
}
if (rejectDuplicateSlowPion_ && duplicateSlowPion) continue;
```

|debugCategoryCutflow|rejectDuplicateSlowPion|Fix04 직전 이 경로의 동작|
|---|---|---|
|false|false|중복 veto 비활성|
|false|true|계산이 생략되어 false 유지: veto를 요청해도 이 경로에서 작동 안 함|
|true|true|debugCat>=0일 때 TrackRef 중복 계산 후 veto 가능|
|true|false|중복 진단만 수행|

Fix05 직전에는 옵션이 없으면 생성자에서 false가 됐다([DStarFitter.cc:112](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:112)). Fix05 직전 canonical MC는 두 옵션을 명시적으로 false로 설정했으며, 검증된 Condor MC dump는 두 옵션을 지정하지 않아 false다. 조사한 data 설정도 해당 veto를 켜지 않았다. **현재 production이 켜진 veto를 잃었다는 주장과, 옵션을 켜도 debug off에서 안 먹는 코드 결함은 구분한다.** Fix05 직전에는 앞의 무조건 TrackRef veto가 주석 처리돼 있었다. 아래 Fix05에서 이 경로를 실제 기본 veto로 적용했다. 이 동일-track 재사용은 서로 다른 후보가 track/GEN decay를 공유하는 후보 중복 집계와도 별개다. 이 결함이 Δm 중앙 peak의 원인이라는 판정이나 발생률 측정은 아직 없다.

### Fix04: duplicate slow-pion veto의 debug 의존성 제거 — 2026-09-29

- 수정 파일: [DStarFitter.cc:469](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:469). 다른 production 소스·설정은 이번 Fix04에서 수정하지 않았다.
- `rejectDuplicateSlowPion_`가 true이면 debug 활성화 및 debug category 유효성에 상관없이 D0 daughter 두 TrackRef와 slow-pion TrackRef를 비교한다. 중복이면 기존 `continue`가 실행된다.
- `debugDuplicateTrack_`는 debug 활성화 및 유효 category 조건에서만 증가한다. `rejectDuplicateSlowPion_ = false`인 현재 조사 설정의 선택 결과는 이 수정으로 바뀌지 않는다.
- [작업 직전→직후 source diff](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix04_duplicate_slow_veto/duplicate_slow_veto.diff) · [검증 기록](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix04_duplicate_slow_veto/verification.json).
- 정적 제어 흐름과 `git diff --check`를 확인했다. CMSSW build, `cmsRun` event 처리, production 제출은 아직 하지 않았다. 기존 라이브러리/ROOT에 이 수정이 반영됐다는 뜻은 아니다.

### Fix05: 동일 TrackRef veto를 무조건 기본 적용 — 2026-09-30

- 준석의 승인: D0 daughter 둘 중 하나와 같은 TrackRef를 slow pion으로 붙인 조합은 debug/옵션과 관계없이 제외한다.
- `DStarFitter.cc`: D0 daughter를 꺼낸 직후 두 TrackRef를 비교하고, 동일하면 mass 계산과 fit 전에 `continue`한다. Fix04의 조건부 veto 블록은 제거했다.
- `DStarFitter.cc/.h`: 사용하지 않게 된 `rejectDuplicateSlowPion_`와 duplicate debug counter/출력을 제거했다. `debugCategoryCutflow` 및 다른 debug 기능은 유지한다.
- Canonical MC Step2 cfg의 `rejectDuplicateSlowPion=False`를 제거했다. Veto는 data/MC 모두 소스 기본 동작이다. 과거 job snapshot과 ROOT를 변경하지 않았다.
- 검증: `git diff --check`, 실제 SCRAM GCC/include 옵션의 `-fsyntax-only`, canonical MC Python syntax 모두 통과. Syntax 검사는 el9 host에서 el8 GCC를 사용했고 SCRAM OS 경고가 있었다. El8 런타임 검증은 미수행이다.
- Producer 라이브러리는 SHA256 대조로 검사 전후 동일함을 확인했다. 실제 CMSSW library build, event 처리, production 제출은 하지 않았다. 후보 수/분포에 대한 실제 영향은 아직 측정하지 않았다.
- [전체 diff](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix05_default_duplicate_slow_veto/default_duplicate_slow_veto.diff) · [검증 JSON](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix05_default_duplicate_slow_veto/verification.json) · [syntax 검사 로그](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix05_default_duplicate_slow_veto/syntax_check.log).

이번 Fix05 작업 직전→직후 전체 diff:

```diff
--- a/VertexCompositeProducer/src/DStarFitter.cc
+++ b/VertexCompositeProducer/src/DStarFitter.cc
@@ -113,8 +113,6 @@
                           theParameters.getParameter<bool>("debugCategoryCutflow");
   debugSlowPionPtScan_ = theParameters.exists("debugSlowPionPtScan") &&
                          theParameters.getParameter<bool>("debugSlowPionPtScan");
-  rejectDuplicateSlowPion_ = theParameters.exists("rejectDuplicateSlowPion") &&
-                             theParameters.getParameter<bool>("rejectDuplicateSlowPion");
   debugLabel_ = theParameters.exists("debugLabel") ? theParameters.getParameter<std::string>("debugLabel") : "DStarFitterDebug";
   if (debugCategoryCutflow_) {
     debugMinDeltaM_.fill(std::numeric_limits<double>::infinity());
@@ -267,8 +265,7 @@
     edm::LogPrint("DStarFitterDebug") << "minDeltaM = " << debugMinDeltaM_[cat]
                                       << ", maxDeltaM = " << debugMaxDeltaM_[cat]
                                       << ", nDeltaM_lt_mPi = " << debugDeltaMLtPionMass_[cat]
-                                      << ", nInvalidMass = " << debugInvalidMass_[cat]
-                                      << ", nDuplicateTrack = " << debugDuplicateTrack_[cat];
+                                      << ", nInvalidMass = " << debugInvalidMass_[cat];
   }
 }
 
@@ -440,16 +437,13 @@
       const reco::Candidate* dau0 = theD0.daughter(0);
       const reco::Candidate* dau1 = theD0.daughter(1);
 
-      // // Skip slow pion if it reuses a track already used in the D0
-      // reco::TrackRef d0Track0;
-      // reco::TrackRef d0Track1;
-      // if (const auto* rc0 = dynamic_cast<const reco::RecoChargedCandidate*>(dau0)) d0Track0 = rc0->track();
-      // if (const auto* rc1 = dynamic_cast<const reco::RecoChargedCandidate*>(dau1)) d0Track1 = rc1->track();
-      // if ((d0Track0.isNonnull() && d0Track0 == pionTrackRef) ||
-      //    (d0Track1.isNonnull() && d0Track1 == pionTrackRef)) {
-
-      //  continue;
-      // }
+      reco::TrackRef d0Track0;
+      reco::TrackRef d0Track1;
+      if (const auto* rc0 = dynamic_cast<const reco::RecoChargedCandidate*>(dau0)) d0Track0 = rc0->track();
+      if (const auto* rc1 = dynamic_cast<const reco::RecoChargedCandidate*>(dau1)) d0Track1 = rc1->track();
+      if ((d0Track0.isNonnull() && d0Track0 == pionTrackRef) ||
+          (d0Track1.isNonnull() && d0Track1 == pionTrackRef))
+        continue;
 
 	      // if( !pionTransTkPtr->impactPointStateAvailable()) continue;
 		      int slowPionCharge = pionTrackRef->charge();
@@ -466,19 +460,6 @@
 		      const int debugCat = debugCategoryCutflow_ ? debugCategoryIndex(kaonCand->charge(), pionCand->charge(), slowPionCharge) : -1;
 		      debugFill(debugCat, kDebugSlowPionAttach);
 		      slowPiPtScanFill(slowPionPtForScan, kSlowPiPtAttach);
-	      bool duplicateSlowPion = false;
-	      if (rejectDuplicateSlowPion_ || (debugCategoryCutflow_ && debugCat >= 0)) {
-	        reco::TrackRef d0Track0;
-	        reco::TrackRef d0Track1;
-	        if (const auto* rc0 = dynamic_cast<const reco::RecoChargedCandidate*>(dau0)) d0Track0 = rc0->track();
-	        if (const auto* rc1 = dynamic_cast<const reco::RecoChargedCandidate*>(dau1)) d0Track1 = rc1->track();
-	        if ((d0Track0.isNonnull() && d0Track0 == pionTrackRef) ||
-	            (d0Track1.isNonnull() && d0Track1 == pionTrackRef)) {
-	          duplicateSlowPion = true;
-	          if (debugCategoryCutflow_ && debugCat >= 0) debugDuplicateTrack_[debugCat]++;
-	        }
-	      }
-	      if (rejectDuplicateSlowPion_ && duplicateSlowPion) continue;
 	      const auto& D0Vec = theD0.p4();
 	      const reco::Track& thePiTrack = pionTransTkPtr->track();
 	      math::PtEtaPhiMLorentzVector pPi(thePiTrack.pt(), thePiTrack.eta(), thePiTrack.phi(), piMassDStar);
--- a/VertexCompositeProducer/interface/DStarFitter.h
+++ b/VertexCompositeProducer/interface/DStarFitter.h
@@ -151,7 +151,6 @@
   bool   useRawDStarKinematics_;
   bool   debugCategoryCutflow_;
   bool   debugSlowPionPtScan_;
-  bool   rejectDuplicateSlowPion_;
   bool   debugHistogramsBooked_ = false;
   std::string debugLabel_;
   enum SlowPionPtScanStage {
@@ -198,7 +197,6 @@
   std::array<double, kDebugNCategory> debugMaxDeltaM_{};
   std::array<unsigned long long, kDebugNCategory> debugInvalidMass_{};
   std::array<unsigned long long, kDebugNCategory> debugDeltaMLtPionMass_{};
-  std::array<unsigned long long, kDebugNCategory> debugDuplicateTrack_{};
   std::array<TH1D*, kDebugNCategory> hDebugRawDStarPt_{};
   std::array<TH1D*, kDebugNCategory> hDebugFitterDStarPt_{};
   std::array<TH1D*, kDebugNCategory> hDebugRawD0Pt_{};
--- a/VertexCompositeProducer/test/production/pbpb2023/mc/PbPb2023_D0BothAndDStar_MB_cfg_mc_v2_Step2MVA.py
+++ b/VertexCompositeProducer/test/production/pbpb2023/mc/PbPb2023_D0BothAndDStar_MB_cfg_mc_v2_Step2MVA.py
@@ -225,7 +225,6 @@
 process.generalDStarCandidatesNew.dPtCut = cms.double(4.5)
 process.generalDStarCandidatesNew.isWrongSign = cms.bool(False)
 process.generalDStarCandidatesNew.useRawDStarKinematics = cms.bool(False)
-process.generalDStarCandidatesNew.rejectDuplicateSlowPion = cms.bool(False)
 process.generalDStarCandidatesNew.debugCategoryCutflow = cms.bool(False)
 process.generalDStarCandidatesNew.debugSlowPionPtScan = cms.bool(False)
 #process.generalDStarCandidatesNew.trkPtSumCut = cms.double(0.0)
```

## DIAG-01: 성공 후보 전후 진단 소스 반영 — 2026-09-30

### 반영 기록

## 결론

준석 승인 범위인 성공 D* 후보의 D*·D0·slow pion 운동학, pT 오차, P4 covariance와 D*–D0 질량 상관값을 추가했다. **46개 ROOT branch 추가: data 147→193, MC 249→295.** 현재는 AFS 소스 반영 및 격리 검사 완료다. Library build, 수정 module의 cmsRun, 새 production physics QA는 수행하지 않았다.

## 수정 위치

|파일·위치|추가 내용|
|---|---|
|[D0Fitter.cc:593](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/D0Fitter.cc:593)|최초 D0 fit의 momentum/mass covariance를 입력 D0 p4 좌표로 변환하여 double userData로 전달|
|[DStarFitter.cc:820](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:820)|최종 mass cut을 통과한 후보에 6개 state 및 2개 질량 상관값 부착|
|[PATCompositeTreeProducer6.h:227](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.h:227)|진단 배열 선언|
|[PATCompositeTreeProducer6.cc:736](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc:736)|D* mode의 branch 선언|
|[PATCompositeTreeProducer6.cc:1300](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc:1300)|후보별 payload 읽기, 운동학·pT 오차·packed covariance·Δm 채우기|
|[FitDiagnostics.h:1](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/interface/FitDiagnostics.h:1)|반복되는 상태 변환·covariance·질량 상관 전파 helper; 신규 파일|

원래 source 줄 삭제/교체는 없으며 additions only이다. 원래 nominal p4, daughter, cut, MVA, matching, event number, EP 연결은 유지했다. Candidate userData 전달 정보는 추가되지만 nominal selection은 바꾸지 않았다.

## 검증 수준

|확인|결과·범위|
|---|---|
|CMSSW GCC syntax-only|D0Fitter.cc / DStarFitter.cc / PAT6.cc 모두 exit 0. Header 포함 검사. Build/link 검사는 아님. 최초 검사에서 initializer braces/ROOT pt2 API를 수정한 뒤 통과.|
|ROOT schema-only 선언 실행|현재 data/MC 조건의 기존 branch 전부 이름·leaflist 동일. D* 각각 +46. Non-D* control +0. Generic storage를 사용해 원래 initTree의 선언·조건을 그대로 ROOT prompt에서 실행; module cmsRun은 아님.|
|Native covariance 대조|기존 CMSSW 라이브러리의 native Kalman CachingVertex와 새 helper를 8개 neutral+charged 수치 입력에서 비교. 최대 상대 차이 4.1829e-15.|
|PAT userData persistence|double P4 / 4×4 symmetric matrix / SVector<double,2>를 TBufferFile로 serialize/read한 뒤 원소별 exact equality. 기존 표준 PAT dictionary 사용; 새 dictionary나 transient-only payload 없음.|
|Δm variance|8개 수치 입력에서 covariance cross term 포함 Δm variance finite/nonnegative. 실제 data/MC candidate 분포 검사는 미실행.|
|Native top covariance vs child 합|native top을 그대로 저장. Native perigee→Cartesian 변환의 float 중간값 때문에 수치 사례에서 두 계산의 최대 상대 차이 1.6583e-7; covariance를 재조정하지 않음.|
|Source/diff|기존 4개 source의 모든 줄 보존 및 whitespace 검사 통과. AFS 파일과 검증한 staged 파일 hash 동일.|
|Library|수정 전 backup과 현재 .so hash 동일. New library build/event 처리 없음.|

수치 검증은 normal synthetic cases에 대한 계산 검증이다. Indefinite covariance 사례, 전체 생산 입력, 선택 효율, 물리 분해능 개선 또는 무편향성의 검증이 아니다.

## covariance 계산 근거와 가정

[CMS Kinematic Vertex Fit guide](https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideKinematicVertexFit), §The KinematicParticle and KinematicVertex:
> “a state of the particle is described by 7 parameters”

Native state `(x,y,z,px,py,pz,m)`와 native 7×7 covariance에서 P4 `(px,py,pz,E)` covariance를 `J C Jᵀ`로 계산한다. `E=sqrt(p²+m²)`이며 energy row의 derivative는 `(px/E,py/E,pz/E,m/E)`다. Before D0는 저장된 입력 candidate의 p4에서 Jacobian을 평가하고, covariance 입력은 최초 D0 native fit의 결과다. 기존 candidate energy의 float 계산/반올림을 새 native mass로 바꾸지 않는다.

[NIST TN 1297](https://www.nist.gov/pml/nist-technical-note-1297/nist-tn-1297-appendix-law-propagation-uncertainty), Appendix A.3, Eq. (A-3):
> “a first-order Taylor series approximation”

각 단계의 Δm 오차는 다음 1차 전파로 계산한다:

```text
g(p4) = (-px/M, -py/M, -pz/M, E/M)
Var(M) = g C_P4 g^T
Var(deltaM) = Var(M_Dstar) + Var(M_D0) - 2 Cov(M_Dstar,M_D0)
```

Before는 서로 다른 daughter TrackRef와 slow pion의 native fit 입력을 독립으로 취급하여 `C_Dstar=C_D0+C_slow`로 저장한다. Native fit이 포함하지 않는 공통 detector/PV nuisance covariance를 새로 모델링하지 않는다.

After의 daughter cross covariance는 현재 unconstrained KinematicParticleVertexFitter의 Kalman smoother에서

```text
C_D0,slow = C_D0(momentum,vertex) C_vertex^-1 C_slow(vertex,momentum)
C_Dstar,D0 = C_D0 + C_slow,D0
Cov(M_Dstar,M_D0) = g_Dstar C_Dstar,D0 g_D0^T
```

로 복원한다. Ground truth는 CMSSW_13_2_11의 `KalmanVertexTrackUpdator.cc`, `KalmanTrackToTrackCovCalculator.cc`, `PerigeeConversions.cc`, `FinalTreeBuilder.cc`와 기존 라이브러리의 CachingVertex cross matrix다. Helper는 native fit algorithm을 교체하거나 두 번째 fit을 수행하지 않는다. 새 mass/PV constraint를 적용한 fitter로 바꾸면 이 복원식을 재검토해야 한다.

기존 covariance를 clipping/regularization/PSD veto/대체 값으로 변경하지 않는다. Native covariance가 유효한 variance를 만들지 못하면 sqrt 결과가 NaN일 수 있고 이를 숨기거나 진단 값을 근거로 후보를 제거하지 않는다. 이 정책은 FIT-01/02/COV-01 유지 결정과 같다.

## 실행·provenance

- 기준 소스: 수정 직전 AFS 파일의 실제 hash. Git HEAD diff가 아님.
- EOS snapshot: [/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/diag01_success_before_after_20260930/before/](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/diag01_success_before_after_20260930/before/) 및 [/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/diag01_success_before_after_20260930/before_libraries/](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/diag01_success_before_after_20260930/before_libraries/).
- 최초 snapshot: 2026-09-30 07:10:44 UTC / 16:10:44 KST. 소스 반영: 07:21:10 UTC / 16:21:10 KST.
- [before_manifest.json](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/diag01_success_before_after_20260930/before_manifest.json), [after_manifest.json](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/diag01_success_before_after_20260930/after_manifest.json), [syntax_results.json](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/diag01_success_before_after_20260930/syntax_results.json), [native_covariance.log](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/diag01_success_before_after_20260930/native_covariance.log), [branch_inventory.json](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/diag01_success_before_after_20260930/branch_inventory.json).
- 세 모듈은 함께 새 source로 build하여 사용해야 한다. 기존 D0 EDM/구 library에는 새 covariance payload가 없다. 현재 Step2는 `generalD0CandidatesNew:D0`를 같은 job에서 만들어 DStarFitter에 전달한다. Legacy-input fallback은 추가하지 않았다.
- Repository-local 임시 test code와 원격 실행 scratch는 결과 문서 저장 후 정리했다. 요청된 backup/diff/schema/검증 log만 보존한다.

## 전체 추가 branch 명세

기준: 2026-09-30 수정 직전 AFS PAT6 소스. `doRecoNtuple && twoLayerDecay && abs(PID)==413`인 D* tree에 추가한다.

|설정|기존|추가|수정 후|
|---|---:|---:|---:|
|PbPb2023 data D* PAT6|147|46|193|
|PbPb2023 MC D* PAT6, reco+GEN|249|46|295|

개수는 현재 `initTree()`의 branch 호출과 조건을 그대로 해석 실행하여 확인했다. 새 CMSSW 모듈의 event 처리로 생성한 ntuple 개수는 아니다. 기존 branch 이름과 leaflist는 전부 보존된다. D0/EP/EventInfo tree에는 새 ROOT branch를 추가하지 않는다.

## 구성

- D*, D0, slow pion × Before/After × Pt/Eta/Phi/Y/Mass/PtErr = 36개.
- `diagDeltaMBefore`, `diagDeltaMAfter` = 2개.
- 입자/단계별 `CovP4` = 6개; 각 branch가 10개 원소 배열이다.
- `diagCovMassDstarD0Before`, `diagCovMassDstarD0After` = 2개.
- 총 46개 branch, 후보당 100개 double = 압축 전 800 bytes 추가. 실제 ROOT 파일 증가량은 후보 수와 압축에 따라 다르다.

`PtErr`는 절대 pT 표준오차이며 GeV 단위다. 상대 오차가 아니다.

## 전체 추가 이름

|번호|branch|타입·형태|단위|
|---|---|---|---|
|1|`diagCovMassDstarD0After`|`double [candSize]`|GeV^2|
|2|`diagCovMassDstarD0Before`|`double [candSize]`|GeV^2|
|3|`diagD0AfterCovP4`|`double [candSize][10]`|GeV^2|
|4|`diagD0AfterEta`|`double [candSize]`|dimensionless|
|5|`diagD0AfterMass`|`double [candSize]`|GeV|
|6|`diagD0AfterPhi`|`double [candSize]`|rad|
|7|`diagD0AfterPt`|`double [candSize]`|GeV|
|8|`diagD0AfterPtErr`|`double [candSize]`|GeV|
|9|`diagD0AfterY`|`double [candSize]`|dimensionless|
|10|`diagD0BeforeCovP4`|`double [candSize][10]`|GeV^2|
|11|`diagD0BeforeEta`|`double [candSize]`|dimensionless|
|12|`diagD0BeforeMass`|`double [candSize]`|GeV|
|13|`diagD0BeforePhi`|`double [candSize]`|rad|
|14|`diagD0BeforePt`|`double [candSize]`|GeV|
|15|`diagD0BeforePtErr`|`double [candSize]`|GeV|
|16|`diagD0BeforeY`|`double [candSize]`|dimensionless|
|17|`diagDeltaMAfter`|`double [candSize]`|GeV|
|18|`diagDeltaMBefore`|`double [candSize]`|GeV|
|19|`diagDstarAfterCovP4`|`double [candSize][10]`|GeV^2|
|20|`diagDstarAfterEta`|`double [candSize]`|dimensionless|
|21|`diagDstarAfterMass`|`double [candSize]`|GeV|
|22|`diagDstarAfterPhi`|`double [candSize]`|rad|
|23|`diagDstarAfterPt`|`double [candSize]`|GeV|
|24|`diagDstarAfterPtErr`|`double [candSize]`|GeV|
|25|`diagDstarAfterY`|`double [candSize]`|dimensionless|
|26|`diagDstarBeforeCovP4`|`double [candSize][10]`|GeV^2|
|27|`diagDstarBeforeEta`|`double [candSize]`|dimensionless|
|28|`diagDstarBeforeMass`|`double [candSize]`|GeV|
|29|`diagDstarBeforePhi`|`double [candSize]`|rad|
|30|`diagDstarBeforePt`|`double [candSize]`|GeV|
|31|`diagDstarBeforePtErr`|`double [candSize]`|GeV|
|32|`diagDstarBeforeY`|`double [candSize]`|dimensionless|
|33|`diagSlowPiAfterCovP4`|`double [candSize][10]`|GeV^2|
|34|`diagSlowPiAfterEta`|`double [candSize]`|dimensionless|
|35|`diagSlowPiAfterMass`|`double [candSize]`|GeV|
|36|`diagSlowPiAfterPhi`|`double [candSize]`|rad|
|37|`diagSlowPiAfterPt`|`double [candSize]`|GeV|
|38|`diagSlowPiAfterPtErr`|`double [candSize]`|GeV|
|39|`diagSlowPiAfterY`|`double [candSize]`|dimensionless|
|40|`diagSlowPiBeforeCovP4`|`double [candSize][10]`|GeV^2|
|41|`diagSlowPiBeforeEta`|`double [candSize]`|dimensionless|
|42|`diagSlowPiBeforeMass`|`double [candSize]`|GeV|
|43|`diagSlowPiBeforePhi`|`double [candSize]`|rad|
|44|`diagSlowPiBeforePt`|`double [candSize]`|GeV|
|45|`diagSlowPiBeforePtErr`|`double [candSize]`|GeV|
|46|`diagSlowPiBeforeY`|`double [candSize]`|dimensionless|

## CovP4 배열 정의

좌표 순서 `(px,py,pz,E)`의 대칭 4×4 covariance 상삼각 10개다. 분산과 공분산을 저장하며 표준편차 배열이 아니다.

|index|원소|
|---:|---|
|0|Cov(px,px)|
|1|Cov(px,py)|
|2|Cov(px,pz)|
|3|Cov(px,E)|
|4|Cov(py,py)|
|5|Cov(py,pz)|
|6|Cov(py,E)|
|7|Cov(pz,pz)|
|8|Cov(pz,E)|
|9|Cov(E,E)|

## Before / After의 뜻

|입자|Before|After|
|---|---|---|
|D0|upstream D0Fitter가 이미 fit하고 저장한 입력 D0 candidate의 p4|D* fit tree의 D0 child `currentState()`|
|slow pion|원래 TrackRef momentum + 기존 pion mass hypothesis|D* fit tree의 slow-pion child `currentState()`|
|D*|Before D0 + Before slow pion의 p4 합|D* fit tree의 top particle `currentState()`|

Before는 세 original track을 모두 처음 fit하기 전 상태가 아니다. 입력 D0 최초 fit은 이미 완료되어 있다. Before→After는 내부 D0 refit과 D* fit의 합산 영향을 포함한다. 내부 D0 refit만 분리할 중간 stage는 이번 명세에 없다.

`diagDeltaMBefore = diagDstarBeforeMass - diagD0BeforeMass`, `diagDeltaMAfter = diagDstarAfterMass - diagD0AfterMass`. After D* energy/mass는 native fit 상태에서 계산한다. 기존 nominal의 first-D0-mass/final-child-momentum 혼합 정의를 새 After에 복사하지 않는다.

## 사용 범위

같은 최종 성공 후보에서 fit 전후 운동학, Δm, 해당 단계의 Δm 오차를 비교하는 용도다. Before histogram에도 기존 prefit/fit/final cut을 모두 통과한 후보만 들어간다. 별도 no-fit 생산의 효율·실패율 분모가 아니다.

위치 covariance, full track covariance, K/pi fit 전후 상태, 단계 사이 covariance 및 full D0–slow-pion cross matrix는 ROOT에 추가하지 않는다. 이 저장만으로 vertex fit을 다시 실행하거나 ΔmAfter−ΔmBefore의 완전한 오차를 계산할 수 있다고 주장하지 않는다. event64·실패/탈락 pair collection·추가 선택·covariance 보정은 제외한다.


## 실제 수정 diff

[source.diff](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/diag01_success_before_after_20260930/source.diff)

```diff
--- a/VertexCompositeProducer/src/D0Fitter.cc
+++ b/VertexCompositeProducer/src/D0Fitter.cc
@@ -15,6 +15,7 @@
 //
 
 #include "VertexCompositeAnalysis/VertexCompositeProducer/interface/D0Fitter.h"
+#include "VertexCompositeAnalysis/VertexCompositeProducer/interface/FitDiagnostics.h"
 #include "CommonTools/CandUtils/interface/AddFourMomenta.h"
 
 #include "TrackingTools/TransientTrack/interface/TransientTrackBuilder.h"
@@ -589,6 +590,7 @@
         std::unique_ptr<CC> theD0 = std::make_unique<CC>();
 
         theD0->setP4(d0P4);
+        theD0->addUserData("diagD0CovP4", vca::fitdiag::covariance(d0Cand->currentState(), d0P4));
         // theD0 = new VertexCompositeCandidate(0, d0P4, d0Vtx, d0VtxCov, d0VtxChi2, d0VtxNdof);
 
         RecoChargedCandidate
--- a/VertexCompositeProducer/src/DStarFitter.cc
+++ b/VertexCompositeProducer/src/DStarFitter.cc
@@ -16,6 +16,7 @@
 
 //#define DEBUG
 #include "VertexCompositeAnalysis/VertexCompositeProducer/interface/DStarFitter.h"
+#include "VertexCompositeAnalysis/VertexCompositeProducer/interface/FitDiagnostics.h"
 #include "CommonTools/CandUtils/interface/AddFourMomenta.h"
 
 #include "TrackingTools/TransientTrack/interface/TransientTrackBuilder.h"
@@ -816,6 +817,28 @@
 		      {
 		        debugFill(debugCat, kDebugFinalMass);
 		        slowPiPtScanFill(slowPionPtForScan, kSlowPiPtFinalMass);
+        // DIAG-01: successful candidates only; nominal p4/daughters and cuts are unchanged.
+        using namespace vca::fitdiag;
+        const P4 d0Before = theD0.p4();
+        const P4 slowBefore(pPi.px(), pPi.py(), pPi.pz(), pPi.energy());
+        const P4 starBefore = d0Before + slowBefore;
+        const CovP4 d0BeforeCov = *theD0.userData<CovP4>("diagD0CovP4");
+        const CovP4 slowBeforeCov = covariance(pionTransTkPtr->initialFreeState(), slowBefore, piMassDStar_sigma);
+        const P4 starAfter = p4(dStarCand->currentState());
+        const P4 d0After = p4(posCand->currentState());
+        const P4 slowAfter = p4(negCand->currentState());
+        const CovP4 d0AfterCov = covariance(posCand->currentState(), d0After);
+        storeState(*theDStar, "DstarBefore", starBefore, d0BeforeCov + slowBeforeCov);
+        storeState(*theDStar, "DstarAfter", starAfter, covariance(dStarCand->currentState(), starAfter));
+        storeState(*theDStar, "D0Before", d0Before, d0BeforeCov);
+        storeState(*theDStar, "D0After", d0After, d0AfterCov);
+        storeState(*theDStar, "SlowPiBefore", slowBefore, slowBeforeCov);
+        storeState(*theDStar, "SlowPiAfter", slowAfter, covariance(negCand->currentState(), slowAfter));
+        MassCross massCross;
+        massCross[0] = massCrossCovariance(starBefore, d0Before, d0BeforeCov, CrossP4());
+        massCross[1] = massCrossCovariance(starAfter, d0After, d0AfterCov,
+                                         fittedCrossCovariance(posCand->currentState(), negCand->currentState()));
+        theDStar->addUserData("diagCovMassDstarD0", massCross);
 		        theDStars.push_back( *theDStar );
          dcaVals_.push_back(cur3DIP.value());
          dcaErrs_.push_back(cur3DIP.error());
--- /dev/null
+++ b/VertexCompositeProducer/interface/FitDiagnostics.h
@@ -0,0 +1,91 @@
+#ifndef VertexCompositeAnalysis_VertexCompositeProducer_FitDiagnostics_h
+#define VertexCompositeAnalysis_VertexCompositeProducer_FitDiagnostics_h
+
+#include <array>
+#include <cmath>
+#include <string>
+
+#include "DataFormats/PatCandidates/interface/CompositeCandidate.h"
+#include "RecoVertex/KinematicFitPrimitives/interface/KinematicState.h"
+#include "TrackingTools/TrajectoryState/interface/FreeTrajectoryState.h"
+
+namespace vca::fitdiag {
+using P4 = reco::Particle::LorentzVector;
+using CovP4 = ROOT::Math::SMatrix<double, 4, 4, ROOT::Math::MatRepSym<double, 4>>;
+using CrossP4 = ROOT::Math::SMatrix<double, 4, 4>;
+using MassCross = ROOT::Math::SVector<double, 2>;
+inline constexpr std::array<const char*, 6> stateNames = {{
+    "DstarBefore", "DstarAfter", "D0Before", "D0After", "SlowPiBefore", "SlowPiAfter"}};
+
+inline P4 p4(const KinematicState& state) {
+  const auto& p = state.kinematicParameters();
+  return P4(p(3), p(4), p(5), std::sqrt(p(3)*p(3) + p(4)*p(4) + p(5)*p(5) + p(6)*p(6)));
+}
+
+// Native state order: (x,y,z,px,py,pz,m); diagnostic order: (px,py,pz,E).
+inline ROOT::Math::SMatrix<double, 4, 7> jacobian(const P4& p) {
+  ROOT::Math::SMatrix<double, 4, 7> j;
+  j(0,3) = j(1,4) = j(2,5) = 1.;
+  j(3,3) = p.px()/p.energy();
+  j(3,4) = p.py()/p.energy();
+  j(3,5) = p.pz()/p.energy();
+  j(3,6) = p.mass()/p.energy();
+  return j;
+}
+
+inline CovP4 covariance(const KinematicState& state, const P4& p) {
+  return ROOT::Math::Similarity(jacobian(p), state.kinematicParametersError().matrix());
+}
+
+inline CovP4 covariance(const FreeTrajectoryState& state, const P4& p, double massSigma) {
+  AlgebraicSymMatrix77 c;
+  const auto& trackCov = state.cartesianError().matrix();
+  for (unsigned int i = 0; i < 6; ++i)
+    for (unsigned int j = i; j < 6; ++j) c(i,j) = trackCov(i,j);
+  c(6,6) = massSigma*massSigma;
+  return ROOT::Math::Similarity(jacobian(p), c);
+}
+
+// The native Kalman smoother correlates distinct inputs through the common vertex:
+// C_ij = C_i(momentum,vertex) * C_vertex^-1 * C_j(vertex,momentum).
+// This applies to the unconstrained KinematicParticleVertexFitter used here.
+inline CrossP4 fittedCrossCovariance(const KinematicState& a, const KinematicState& b) {
+  const auto& ca = a.kinematicParametersError().matrix();
+  const auto& cb = b.kinematicParametersError().matrix();
+  AlgebraicSymMatrix33 vertexCov;
+  ROOT::Math::SMatrix<double, 7, 3> av;
+  ROOT::Math::SMatrix<double, 3, 7> vb;
+  for (unsigned int i = 0; i < 3; ++i) {
+    for (unsigned int j = i; j < 3; ++j) vertexCov(i,j) = ca(i,j);
+    for (unsigned int j = 0; j < 7; ++j) {
+      av(j,i) = ca(j,i);
+      vb(i,j) = cb(i,j);
+    }
+  }
+  int inversionStatus = 0;
+  const auto vertexInverse = vertexCov.Inverse(inversionStatus);
+  return jacobian(p4(a)) * av * vertexInverse * vb * ROOT::Math::Transpose(jacobian(p4(b)));
+}
+
+inline ROOT::Math::SVector<double, 4> massGradient(const P4& p) {
+  return {-p.px()/p.mass(), -p.py()/p.mass(), -p.pz()/p.mass(), p.energy()/p.mass()};
+}
+
+// Cov(M(D*),M(D0)), with C_Dstar,D0 = C_D0 + C_slow,D0.
+inline double massCrossCovariance(const P4& parent, const P4& d0, const CovP4& d0Cov,
+                                 const CrossP4& d0SlowCross) {
+  return ROOT::Math::Dot(massGradient(parent),
+                        (d0Cov + ROOT::Math::Transpose(d0SlowCross)) * massGradient(d0));
+}
+
+inline double ptError(const P4& p, const CovP4& c) {
+  return std::sqrt((p.px()*p.px()*c(0,0) + 2.*p.px()*p.py()*c(0,1) + p.py()*p.py()*c(1,1))/(p.pt()*p.pt()));
+}
+
+inline void storeState(pat::CompositeCandidate& candidate, const char* name, const P4& p, const CovP4& c) {
+  candidate.addUserData(std::string("diag") + name + "P4", p);
+  candidate.addUserData(std::string("diag") + name + "CovP4", c);
+}
+}  // namespace vca::fitdiag
+
+#endif
--- a/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc
+++ b/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc
@@ -1,4 +1,5 @@
 #include "VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.h"
+#include "VertexCompositeAnalysis/VertexCompositeProducer/interface/FitDiagnostics.h"
 
 #include <algorithm>
 #include <array>
@@ -732,6 +733,23 @@
     PATCompositeNtuple_->Branch("bestvtxY", &bestvy, "bestvtxY/F");
     PATCompositeNtuple_->Branch("bestvtxZ", &bestvz, "bestvtxZ/F");
     PATCompositeNtuple_->Branch("candSize", &candSize, "candSize/I");
+    if (twoLayerDecay_ && std::abs(pid_) == 413) {
+      for (unsigned int i = 0; i < diagStates_.size(); ++i) {
+        auto& d = diagStates_[i];
+        const std::string name = std::string("diag") + vca::fitdiag::stateNames[i];
+        PATCompositeNtuple_->Branch((name+"Pt").c_str(), d.pt, (name+"Pt[candSize]/D").c_str());
+        PATCompositeNtuple_->Branch((name+"Eta").c_str(), d.eta, (name+"Eta[candSize]/D").c_str());
+        PATCompositeNtuple_->Branch((name+"Phi").c_str(), d.phi, (name+"Phi[candSize]/D").c_str());
+        PATCompositeNtuple_->Branch((name+"Y").c_str(), d.y, (name+"Y[candSize]/D").c_str());
+        PATCompositeNtuple_->Branch((name+"Mass").c_str(), d.mass, (name+"Mass[candSize]/D").c_str());
+        PATCompositeNtuple_->Branch((name+"PtErr").c_str(), d.ptErr, (name+"PtErr[candSize]/D").c_str());
+        PATCompositeNtuple_->Branch((name+"CovP4").c_str(), d.covP4, (name+"CovP4[candSize][10]/D").c_str());
+      }
+      PATCompositeNtuple_->Branch("diagDeltaMBefore", diagDeltaMBefore_, "diagDeltaMBefore[candSize]/D");
+      PATCompositeNtuple_->Branch("diagDeltaMAfter", diagDeltaMAfter_, "diagDeltaMAfter[candSize]/D");
+      PATCompositeNtuple_->Branch("diagCovMassDstarD0Before", diagCovMassDstarD0Before_, "diagCovMassDstarD0Before[candSize]/D");
+      PATCompositeNtuple_->Branch("diagCovMassDstarD0After", diagCovMassDstarD0After_, "diagCovMassDstarD0After[candSize]/D");
+    }
     if (isCentrality_) PATCompositeNtuple_->Branch("centrality", &centrality, "centrality/I");
 
     if (isEventPlane_) {
@@ -1279,6 +1297,28 @@
     resetCandidateOutputs(it);
     ++nRecoCandidatesProcessed_;
     const CC& trk = (*v0candidates_)[it];
+    if (twoLayerDecay_ && std::abs(pid_) == 413) {
+      for (unsigned int i = 0; i < diagStates_.size(); ++i) {
+        auto& d = diagStates_[i];
+        const std::string name = std::string("diag") + vca::fitdiag::stateNames[i];
+        const auto& p = *trk.userData<vca::fitdiag::P4>(name+"P4");
+        const auto& c = *trk.userData<vca::fitdiag::CovP4>(name+"CovP4");
+        d.pt[it] = p.pt();
+        d.eta[it] = p.eta();
+        d.phi[it] = p.phi();
+        d.y[it] = p.Rapidity();
+        d.mass[it] = p.mass();
+        d.ptErr[it] = vca::fitdiag::ptError(p, c);
+        unsigned int packed = 0;
+        for (unsigned int row = 0; row < 4; ++row)
+          for (unsigned int col = row; col < 4; ++col) d.covP4[it][packed++] = c(row,col);
+      }
+      diagDeltaMBefore_[it] = diagStates_[0].mass[it] - diagStates_[2].mass[it];
+      diagDeltaMAfter_[it] = diagStates_[1].mass[it] - diagStates_[3].mass[it];
+      const auto& massCross = *trk.userData<vca::fitdiag::MassCross>("diagCovMassDstarD0");
+      diagCovMassDstarD0Before_[it] = massCross[0];
+      diagCovMassDstarD0After_[it] = massCross[1];
+    }
 
     const reco::Candidate* d1 = trk.daughter(0);
     const reco::Candidate* d2 = trk.daughter(1);
--- a/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.h
+++ b/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.h
@@ -224,6 +224,21 @@
   std::vector<double> dstarMassHistHiBins_;
   std::vector<double> dstarMassHistDcaBins_;
   std::vector<double> dstarMassHistMvaCuts_;
+
+  struct FitDiagnosticState {
+    double pt[kMaxGenCand];
+    double eta[kMaxGenCand];
+    double phi[kMaxGenCand];
+    double y[kMaxGenCand];
+    double mass[kMaxGenCand];
+    double ptErr[kMaxGenCand];
+    double covP4[kMaxGenCand][10];
+  };
+  std::array<FitDiagnosticState, 6> diagStates_;
+  double diagDeltaMBefore_[kMaxGenCand];
+  double diagDeltaMAfter_[kMaxGenCand];
+  double diagCovMassDstarD0Before_[kMaxGenCand];
+  double diagCovMassDstarD0After_[kMaxGenCand];
 
   int Ntrkoffline;
   int Npixel;
```


## DIAG-01: 왜 44개 branch를 제안했는가 — 과거 미구현 명세

아래 44개는 과거 미구현 설계 기록이다. 위의 현재 46개 명세와 구분한다. 현재 구현에는 event64·실패 pair 저장이 없다.


**2026-09-30 현재 결정:** 아래 44개는 결정 전 제안의 기록이다. EV-03은 기존 32-bit 유지로 결정했으므로 `diagEvent64` 추가는 채택하지 않는다. 아래 표/명세가 그대로 승인된 구현 목록이라는 뜻은 아니며, FIT-06의 현재 저장 방식 유지 결정에 따라 실패 pair의 추가 출력도 미채택이다. 필요한 성공 후보 진단 field의 추가 여부만 별도 판단 대상이다.

**44개를 이미 추가했다는 뜻도, 모든 분석에 44개가 필수라는 뜻도 아니다.** 중앙 이동만 보면 Δm before/after와 δmDStar/δmD0의 네 scalar가 우선이다. 추가안은 이동 원인을 track·fit 단계·covariance·후보 중복별로 추적하고 기존 EP/weight와 같은 event를 연결하기 위해 만든 구체적인 저장 설계다.

|저장 위치/목적|추가 branch object 수|
|---|---:|
|DStar: event/LFN/candidate/D0/track 식별|9|
|DStar: K·π·slow·D0·DStar의 단계별 state 배열|13|
|DStar: nominal/legacy p4|4|
|DStar: Δm·δm scalar|5|
|DStar: fit status/χ²/ndf/vertex/refit·pass mask|6|
|DStar: covariance 배열과 validity|4|
|DStar 합계|41|
|기존 D0 tree: diagEvent64|1|
|기존 custom eventplane tree: diagEvent64|1|
|기존 EventInfo tree: diagEvent64|1|
|전체 data 파일 합계|44|

배열 원소는 별도의 branch로 세지 않는다. 44개 tree를 만들자는 뜻이 아니다. 41개 안의 DStar event64와 다른 세 tree의 event64를 함께 저장해야 32-bit EventNb 유실 없이 연결할 수 있다.

아래는 [별도 명세](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/BRANCH_SCHEMA.md)의 41개 목록과 범위 설명을 이 문서에 직접 수록한 것이다. **현재 AFS에 이 branch 추가는 구현하지 않았다.** 구현한다면 D0/DStar fitter의 상태 payload와 PAT6 `.h`/`.cc` 및 다른 tree의 event64 저장 수정이 필요하다. EventInfo의 HLT/evtSel 설정만 고친 Fix02/03과는 다른 작업이다.

|#|그룹|이름|형식|정의|
|---:|---|---|---|---|
|1|identity|`diagEvent64`|uint64/event|Full EDM event number; existing RunNb/LSNb retained|
|2|identity|`diagInputFile`|string/event|Input LFN; not output file or just basename|
|3|identity|`diagCandidateIndex`|int64/candidate|Legacy accepted candidate index; -1 if not accepted|
|4|identity|`integrityD0Index`|uint32/candidate|Index in frozen first-D0 collection|
|5|identity|`diagTrackKey`|uint32[3]|Unpacked track keys, order K, pi, slow|
|6|identity|`diagTrackProductID`|uint32[3]|Track ProductID diagnostic; not global identity|
|7|identity|`diagPackedSource`|uint8[3]|0 packedPFCandidates, 1 lostTracks, 2 lostTracks:eleTracks, 255 unavailable|
|8|identity|`diagPackedKey`|uint32[3]|Key in source packed collection via existing unpacker track-to-packed Ptr vector|
|9|identity|`diagTrackReuse`|bool|Any same input track used in more than one daughter role|
|10|states|`diagKOriginal`|double[10]: px,py,pz,E,charge,ptError,mass,x,y,z|Original unpacked K TrackRef, before all fits|
|11|states|`diagPiOriginal`|double[10]: px,py,pz,E,charge,ptError,mass,x,y,z|Original unpacked pi TrackRef, before all fits|
|12|states|`diagSlowOriginal`|double[10]: px,py,pz,E,charge,ptError,mass,x,y,z|Original unpacked slow TrackRef, before all fits|
|13|states|`diagKFirst`|double[10]: px,py,pz,E,charge,ptError,mass,x,y,z|K child state after first D0 fit|
|14|states|`diagPiFirst`|double[10]: px,py,pz,E,charge,ptError,mass,x,y,z|pi child state after first D0 fit|
|15|states|`diagKInternal`|double[10]: px,py,pz,E,charge,ptError,mass,x,y,z|K child state after internal D0 refit in DStarFitter|
|16|states|`diagPiInternal`|double[10]: px,py,pz,E,charge,ptError,mass,x,y,z|pi child state after internal D0 refit in DStarFitter|
|17|states|`diagSlowInput`|double[10]: px,py,pz,E,charge,ptError,mass,x,y,z|Slow kinematic particle state immediately before DStar vertex fit, after transient-track state conversion|
|18|states|`diagSlowRefit`|double[10]: px,py,pz,E,charge,ptError,mass,x,y,z|Slow child state after final DStar fit|
|19|states|`diagD0First`|double[10]: px,py,pz,E,charge,ptError,mass,x,y,z|First D0 fit parent kinematic state (not rounded nominal candidate)|
|20|states|`diagD0Internal`|double[10]: px,py,pz,E,charge,ptError,mass,x,y,z|Internal D0 parent immediately before DStar vertex fit|
|21|states|`diagD0Refit`|double[10]: px,py,pz,E,charge,ptError,mass,x,y,z|D0 child after final DStar fit; do not label its K/pi children as newly refitted|
|22|states|`diagDStarFit`|double[10]: px,py,pz,E,charge,ptError,mass,x,y,z|Final DStar parent kinematic state|
|23|nominal_states|`diagD0OriginalP4`|double[4]: px,py,pz,E|Exact first stored D0 candidate p4|
|24|nominal_states|`diagLegacyD0P4`|double[4]: px,py,pz,E|Final fitted D0 momentum with first stored D0 mass, as used in nominal parent energy|
|25|nominal_states|`diagLegacySlowP4`|double[4]: px,py,pz,E|Exact nominal stored slow daughter p4|
|26|nominal_states|`diagLegacyDStarP4`|double[4]: px,py,pz,E|Exact nominal stored DStar parent p4; legacy boost reference|
|27|mass|`deltaMOriginal`|double, GeV|M(first stored D0 + original slow)-M(first stored D0)|
|28|mass|`deltaMRefit`|double, GeV|M(final D0 child + final slow child)-M(final D0 child)|
|29|mass|`diagDeltaMNominal`|double, GeV|Legacy nominal parent mass minus first stored D0 mass|
|30|mass|`deltaMDStar`|double, GeV|M(final children sum)-M(first stored D0 + original slow)|
|31|mass|`deltaMD0`|double, GeV|M(final D0 child)-M(first stored D0)|
|32|fit_status|`diagFitStatus`|uint16[3]|First D0, internal D0, final DStar stage; bits attempted/tree/state/vertex/daughters valid; not attempted distinct from failure|
|33|fit_status|`diagFitChi2`|double[3]|Raw fit chi2; retain negative/nonfinite values|
|34|fit_status|`diagFitNdf`|double[3]|Raw fit ndf|
|35|fit_status|`diagFitVertex`|double[3][3]|Vertex xyz in cm for each fit stage|
|36|fit_status|`diagTrackRefitMask`|uint8[3]|Per K,pi,slow role stage bitmask; no fictitious K/pi update at DStar stage|
|37|fit_status|`diagPassMask`|uint32|Legacy acceptance and named alternative acceptance/evaluated bits; proposal does not define or run alternative algorithm|
|38|covariance|`diagTrackOriginalCov`|double[3][25]|Row-major original 5x5, parameter order q/p,lambda,phi,dxy,dsz|
|39|covariance|`diagFitStateCov`|double[10][49]|Row-major 7x7 x,y,z,px,py,pz,m for non-original states in states list, including slow fit input|
|40|covariance|`diagVertexCov`|double[3][9]|Row-major 3x3 xyz covariance for each fit stage|
|41|covariance|`diagCovarianceValidity`|uint8[16]|3 original track + 10 fit state + 3 vertex matrices; unavailable/nonfinite/indefinite/PSD-singular/PD status, no matrix correction|

공통 단위: momentum/mass/energy GeV, position cm, 각도 rad. state 배열의 순서와 covariance 좌표 순서는 다르므로 schema metadata를 반드시 함께 저장한다. Fit-state ptError는 해당 covariance에서 전파한 값이며 original track ptError로 대체하지 않는다. Invalid/indefinite state도 기록하고 자동 보정하지 않는다.

Constraint 종류·mass hypothesis·mass sigma·fitter tolerance·GT/IOV·ONNX hash·feature 순서·code/config hash·EP 제외 후보 정의는 job 단위 metadata/TNamed/JSON으로 한 번 기록한다. 이들은 위 branch 수에 포함하지 않는다.

이 명세는 기존 성공 후보의 before/after 비교용이다. 실패/신규 유입까지 보려면 prefit pair diagnostic collection을 별도로 내보내야 한다. 독립적인 candidate-row PairDiagnostics tree로 만들 경우 위 41개 필드에 run/lumi 2개를 더한 43필드 구조를 쓸 수 있으며, 이것은 위 44개 branch 추가안과 별도 확장이다. 원래 prefit 선택을 통과하지 못한 조합까지 포함한다는 뜻은 아니다.

Cross-covariance, sigmaDeltaM, availability/status를 실제 구현해 저장할 때는 별도 3개 branch를 추가한다. 현재 주변 covariance만으로 독립 가정하여 sigmaDeltaM을 채우지 않는다. Core fitter의 CachingVertex 단계에서 track-to-track covariance를 노출해야 한다. 미구현 값을 available=true로 표시하지 않는다.

MC GEN 확장은 이 data 41개 계산에 포함하지 않는다. 기존 matchGEN을 보존하고 GEN index/PDG/p4/ancestor/matching deltaR/origin을 별도로 연결해야 한다. 기존 EOS full diagnostic patch의 GEN branches를 재사용할 수 있다.

선택 기준을 바꾸지 않는 진단 저장만으로도 D0Fitter와 DStarFitter에 상태 payload를 추가해야 한다. Producer 알고리즘 교체나 covariance 보정이 필요하다는 뜻은 아니다.


## 조사 근거 보충: 실제 호출 경로와 nominal 상태

1. `changeToMiniAOD` inserts `unpackedTracksAndVertices` and rewrites generalTracks/offlinePrimaryVertices InputTags to its outputs: [PATAlgos_cff.py:133](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/python/PATAlgos_cff.py:133).
2. Unpacker takes packedPFCandidates/lostTracks/eleTracks. For tracks with details it copies pseudoTrack reference point, momentum, charge, covariance; chi2/ndof are separately reconstructed. No positive-definite projection is applied: [TrackAndVertexUnpacker.cc:75](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/plugins/TrackAndVertexUnpacker.cc:75). It already outputs the track→packed Ptr vector, so stable input collection/key can be recorded **without changing the unpacker algorithm**: [TrackAndVertexUnpacker.cc:174](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/plugins/TrackAndVertexUnpacker.cc:174).
3. `D0Producer::produce → D0Fitter::fitAll`: original K/pi TransientTracks → first D0 common-vertex fit → first D0/children candidate → MVA → accepted D0 collection. [D0Fitter.cc:445](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/D0Fitter.cc:445).
4. `DStarProducer::produce → DStarFitter::fitAll`: frozen accepted D0 × slow-track pairs → prefit gates → **K/pi are refit again from original TrackRefs** → internal D0 composite + slow particle → DStar common-vertex fit. [DStarProducer.cc:48](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarProducer.cc:48), [DStarFitter.cc:597](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:597), [DStarFitter.cc:619](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:619).
5. Final fitted D0 and slow states update their pT/η/φ. K/pi are not independently refit again by this DStar fit. CMSSW `FinalTreeBuilder` replaces the D0 top particle and attaches its existing subtree; this is not a global three-track cascade smoothing: [FinalTreeBuilder.cc:105](/cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src/RecoVertex/KinematicFit/src/FinalTreeBuilder.cc:105).
6. Successful selected candidate is published; PAT6 copies its nominal values. Custom eventplane receives the DStar collection, while official hiEvtPlaneRecalc does not: [PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:300](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:300), [PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:341](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:341).

Both D0 fits and the DStar fit call `KinematicParticleVertexFitter`, with no explicit D0/DStar mass constraint and no PV/beamspot vertex constraint. Particle mass hypotheses with mass uncertainties are supplied; these are not a parent mass constraint. PV is used afterwards for impact/flight/pointing calculations and selection. The numerical seed covariance is a loose 10000 cm² diagonal, not a measured PV constraint: [KinematicParticleVertexFitter.cc:47](/cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src/RecoVertex/KinematicFit/src/KinematicParticleVertexFitter.cc:47).

|저장 대상|현재 nominal|전후 비교에 별도로 필요한 상태|
|---|---|---|
|DStar parent|Final parent momentum; energy from final D0 momentum + first stored D0 mass, and final slow momentum + fixed pion mass|Raw first-D0+slow p4; internal-D0+slow input; full final child p4 sum; final parent kinematic state|
|D0 daughter|First stored D0 candidate retained unchanged|Internal D0 before DStar fit and final D0 child|
|Slow daughter|Final fitted momentum and fixed pion-mass energy, but TrackRef remains original|Original track and actual prefit kinematic particle, plus final child|
|K/pi daughter|First-D0 stored daughters retained; original TrackRefs|Original, first fit, internal D0 refit; no invented final-DStar K/pi fit state|
|`pTerrD2` and daughter track qualities|Original TrackRef values, even when daughter momentum is fitted|Fit-state propagated pT error separately|

Code: [DStarFitter.cc:684](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:684), [DStarFitter.cc:788](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:788), [DStarFitter.cc:804](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:804), [PATCompositeTreeProducer6.cc:1508](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc:1508), [PATCompositeTreeProducer6.cc:1597](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc:1597), [PATCompositeTreeProducer6.cc:1635](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc:1635). Nominal's mixed definitions are confirmed; this by itself does not prove the central-bin cause. Nominal p4 construction also uses float energies, so exact legacy reproduction must preserve its arithmetic as well as the conceptual formula.


## 진단 저장/fit 교체 시 수정 위치 — 미구현 범위

|목적·위치|필요한 변경안|불필요하거나 미결정인 부분|
|---|---|---|
|전후 저장: D0Fitter|최초 fit state/covariance/vertex payload를 candidate에 추가|D0 selection/model/fit 변경은 불필요|
|전후 저장: DStarFitter|원본·internal D0·slow fit 입력·최종 state/metadata 추가; legacy 계산 유지|Covariance clipping이나 constraint 추가를 채택하지 않음|
|실제 DStar fit 교체/생략|pair loop, fit 호출, 후속 cut, parent/daughter 정의를 명시적으로 분기|useRawDStarKinematics만으로 불가능; 교체 알고리즘은 미결정|
|DStarProducer|기존 성공 candidate userdata는 copy/put으로 전달. 실패 pair의 별도 diagnostic collection은 FIT-06 결정으로 미채택|성공 후보 진단 field의 추가 여부만 미확정|
|PAT6 .cc/.h|Branch 선언, 초기화/reset, payload 읽기; EV-03 결정으로 기존 32-bit key 유지|진단 field 범위 미확정; 기존 nominal branch 보존|
|Unpacker|기존 track→packed association으로 source/key 기록|Track covariance나 생성 알고리즘 변경 불필요|
|Event plane|비교 기준인 legacy 제외 후보 집합/Q/calibration 고정. 새 집합의 EP는 별도 비교. EV-03 결정으로 기존 32-bit key 유지|Fit 변경 때문에 official EP 계산식까지 바꿀 필요는 없음|
|PSet/metadata|저장 모드, LFN, GT/IOV, code/library/ONNX hash, constraint/mass sigma 고정·기록|서로 다른 data cfg를 같은 production 설정으로 취급하면 안 됨|

기존 ROOT에 원래 track/state가 없으면 나중에 정확한 full refit 비교를 복구할 수 없다. 원본 MiniAOD가 있으면 그 입력으로 새 tree를 만들 수 있다. MiniAOD pseudoTrack의 정보량은 full RECO와 동일하지 않다. 저장된 진단 state를 사용하는 제한된 study와 전체 data/MC reconstruction을 구분한다.


## 조사 범위와 문서 완전성

이 목록은 연결된 read-only 감사 보고서의 확인 사항을 모아 관리하는 문서이며, 코드 전체의 무결성이나 모든 과거 production의 동일성을 보증하는 문서가 아니다. 이번 재대조에서 MC 이력의 누락을 수정하고 branch 전체 명세/변경 위치를 본문에 포함했다. 원래 보고서는 수정 전 기록으로 유지한다.

|감사 보고서의 범위|이 문서의 관리 항목|
|---|---|
|§1 현재 AFS/과거 job 설정 차이|CFG-02, OPS-01, Fix02/03|
|§2 호출 경로·nominal/fit 상태|NT-01/02, 위 호출 경로 보충|
|§3A covariance·χ²·mass sigma·raw 옵션·중복·실패·prefit cut·수렴|FIT-01~07, COV-01|
|§3B event ID·상한·오차·DCA·reset·GEN·MVA label|EV-03, NT-02~05, MC-01, CFG-01|
|§3C EP p/m·HLT·offline flags·후보 제외·Q/보정·PV/DB|EP-01~05, EV-01/02/04, Fix02/03|
|§3D ONNX feature 의미·job provenance|MVA-01, OPS-01|
|§4 수정 위치·§5 41+3 branch·추가 확장|DIAG-01 본문 명세와 수정 위치 표|

MVA-01의 구체적 20개 입력은 `pT, y, centrality, VtxProb, 3DCosPointingAngle, 3DPointingAngle, 2DCosPointingAngle, 2DPointingAngle, 3DDecayLength, 3DDecayLengthSignificance, 2DDecayLength, 2DDecayLengthSignificance, pTD1, EtaD1, pTerrD1, pTD2, EtaD2, pTerrD2, Trk3DDCA, dEta_dau`다. D1/D2는 D0 fitter의 양/음전하 역할이며 고정 K/π가 아니다. Slow pion·EP·cos를 직접 입력하지 않아도 상관관계를 통한 선택 편향은 가능하다. 두 번째 DStar fit만 바꾸고 첫 D0/model을 고정하면 D0 score 자체를 직접 재계산하는 변경은 아니지만, 후보 선택과 mass/cos 응답은 달라질 수 있다.

EP 관련 추가 정의: official hiEvtPlaneRecalc는 DStar collection을 직접 입력받지 않으며 packed/lost track 경로를 쓴다. Custom EP의 daughter 제외는 TrackRef ProductID/key 기준이고 geometric fallback은 없다. Official packed 경로에서 HF는 packed 후보에서 가져오므로 `caloTag=particleFlow`라는 설정만으로 다른 PF 경로 사용을 단정하지 않는다. DB/IOV의 실제 보정 적합성은 이 정적 감사로 검증하지 않았다.

남은 검증: 실제 data/MC event 처리, 기존 branch·후보·key·EP의 일치 검사, 바뀐 event selection의 효율과 GEN 분모 범위, 과거 job-time PSet/library 연결. 설정 load 통과와 production 검증 완료를 구분한다. Cross-covariance/σ(Δm), 실패 pair 저장, 64-bit event key와 44개 branch는 아직 구현 완료가 아니다.

<a id="current-findings"></a>

## 조사 전체를 읽기 위한 현재 결론과 범위

이번 통합은 기존 조사 자료를 이 문서 **본문에 모두 수록**하는 문서 보완이다. 새 fit, production 수정, 제출, covariance 처방을 실행한 기록이 아니다. 아래 완료 결과는 각 보존 보고서·산출물의 기록에 근거하며, 이번 편집에서 그 계산을 모두 다시 실행했다는 뜻이 아니다. 각 원문 절에는 원본 위치·SHA256·완료/계획 구분을 붙였다. 반복되는 원문도 누락 방지를 위해 보존했다.

- **수정 완료와 검증 완료를 구분:** PAT6 tracker p/m 소스 수정, data Step2 2곳·MC Step2 2곳 설정 수정은 확인했다. EP 수정의 build/event 검증과 새 event-selection의 실제 event 처리는 아직 완료로 기록할 수 없다. 설정 로드 통과와 production 반영은 다르다.
- **중앙 bin 증가 ≠ 전체 resolution 개선:** refit 후 중앙-bin 비율이나 fitted FWHM만으로 전체 질량 정확도를 판정하지 않는다. weighted Q84−Q16은 전체 68% 폭이며 half-width가 아니다. FWHM, GEN 잔차 폭, 중앙-bin 비율의 정의를 따로 보존했다.
- **표본을 섞지 않는다:** 9/19 PR 2,288개 EP 기준 5개 cos bin, 9/29 official PR/NPR 1,281개 tracker-axis 목표 구간, 각 단계의 1,264/1,229개 재현 부분집합, 6,159개 전 cos GEN-mass 비교, private RECO 14/108개 benchmark는 서로 다른 비교다. 아래 표본 수 차이를 단순 모순이나 후보 손실 하나로 합치지 않는다.
- **원본 track 부재의 범위:** official 문제 후보의 원본 RECO track이 없는 제한과, 별도 보존 private RECO를 활용한 14/108개 benchmark 완료를 구분한다. MiniAOD unpacked track 재구성이나 posdef 행렬은 잃어버린 original RECO의 복원이 아니다.
- **과거 미완료 문구의 정리:** PR EP 5개 cos 비교는 이후 완료됐다. 최초 REPORT의 proxy/미제출 문구는 SUBMISSION_REPORT로 갱신됐다. target_bin_posdef 최초 제출 보고서는 제출 시점 기록이며, 실제 사용 가능한 corrected 표본은 후속 truth_geometry 보고서의 수락·누락 목록에 따른다. 전체 ONNX 입력 복원은 후속 bdt_input_attribution에서 진행됐다. 과거 계획의 승인 요청은 지금의 새 요청이 아니다.
- **확정하지 않은 것:** covariance 문제만이 peak의 원인이라는 주장, fitting 절차 전체가 정상이라는 주장, 특정 BDT 입력 하나가 유일 원인이라는 주장, no-refit만으로 mass→DCA 추출이 유효하다는 주장은 어느 것도 확정하지 않았다. 임의 clipping·후보 제거·PV 제약·전체 data/MC 재생산을 이 문서 보완에서 채택하지 않았다.

### 조사 결과 대장 — 수정 목록과 별도로 완료된 물리·수치 검증

각 행의 전체 수치·코드 근거·대표 event·실패·산출물은 뒤의 동일 경로 원문에 모두 수록했다.

|ID|조사 / 완료 범위|확인 결과|아직 말할 수 없는 것 / 적용 상태|원문 경로 (진단 디렉터리 기준)|
|---|---|---|---|---|
|INV-01|진단 production 구현·동일 입력 검증·CRAB 제출|기존 5개 tree 유지, D* 기존 249 + 진단 299 branches. PR4/NPR4/PR0 기존 모든 branch 동일 검증 후 3개 제출 수락. daughter cross-covariance·σΔm unavailable 명시.|제출 수락을 모든 worker 완료로 바꾸어 읽지 않는다. 새 data 44개 제안과 별개.|README.md, REPORT.md, SUBMISSION_REPORT.md|
|INV-02|원래 covariance 저장·fit 입력 추적|track 5×5와 kinematic 7×7의 좌표 변환·저장 순서·실제 입력을 대조. 대표 indefinite 행렬이 fit 직전에도 남는 것을 확인.|branch 저장 오류만으로 설명할 수 없으며 PSD 여부만으로 결과 정확도를 보장하지 않는다.|audit_covariance/REPORT.md|
|INV-03|대표 8후보 exact replay|선택한 indefinite 6개에서 Cholesky 실패→일반 역행렬→valid. PSD PR 144.637→145.508, NPR 146.387→145.313 MeV 중앙 이동도 확인.|8개 선택 표본은 전체 발생률이나 유일 원인을 주지 않는다.|handoff_replay_20260919/REPORT.md|
|INV-04|PR EP 5개 cos 2,288개 완료|중앙 이동은 주로 vertex fit 단계. 목표 501개 15.152→19.999→20.200%, width68 1.299→1.445→1.446 MeV.|목표 구간이 다른 구간보다 유독 크다는 유의성은 제한적. 전체 폭이 좁아졌다는 설명은 철회.|three_stage_pr4_all_cos_20260919/REPORT.md|
|INV-05|GEN 방향·matching·중복 대조|전체 slow ΔR 개선 가중 비율 51.09%; 목표 중앙 유입은 70.93%. 전체 목표 구간 RMS는 증가. 선택 표본의 동일 slow track 반복 0, 동일 GEN D* 공유 292후보/145그룹.|독립 hit truth 아님. 입력 표본 밖 unmatched·matching 대안 전체는 미검증.|slow_gen_response_20260919/REPORT.md 및 INTERPRETATION.md|
|INV-06|BDT threshold replay와 20개 입력|24대표 replay에서 0.95 cut은 12개를 제외하고 살아남은 12개의 31개 값은 동일. 실제 모델/입력 정의·score를 추적.|threshold가 생존 후보의 refit 값을 직접 바꾼다는 증거 없음. 선택과 후속 fit 반응의 연결을 검사해야 함.|bdt_refit_causality_20260929/REPORT.md|
|INV-07|packing과 보존 RECO 대조|동일 original RECO→packed에서 PSD 손실 확인. private 108 slow 중 60, D0 daughter 216 중 10 indefinite; original 324행렬은 모두 PD.|private 표본의 비율을 official 문제 cos 모집단에 적용하지 않는다. PD 자체는 coverage 검증 아님.|EXISTING_RECO_REPORT.md, PILOT_COVARIANCE_REPORT.md (bdt_refit_causality_20260929 아래)|
|INV-08|공식 pseudoPosDefTrack 개입|private 108개 slow 60→0, D0 daughter 10→0 indefinite. 실제 full reconstruction 공통108/유입0/손실0, BDT threshold crossing0. Original-RECO refit과 >0.1MeV 차이는 27→25개로 남음.|posdef는 original covariance 복구가 아님. target 491 PR pass 후속 대조도 좁은 core를 제거하지 못함.|official_posdef_108/REPORT.md, truth_geometry/REPORT.md (BDT 조사 아래)|
|INV-09|cascade 방법 검토|Xi→Λπ, Ω→ΛK 등 composite+track 사례와 CMSSW B→J/ψK 경로 검토. D0 비행선+slow common vertex는 가능한 구성. 작은 opening은 종방향 제약을 약하게 함.|유사 channel 존재가 D* vertex 오차의 보증은 아님. NPR에 PV를 생성점으로 강제하는 처치는 채택 안 함.|cascade_method_review_20260929/REPORT.md|
|INV-10|geometry·GEN 기준점 진단|1264개 기하 재현; 입력→fitted vertex로 전파→fit의 내부 Δm 중앙 비율 PR pass 15.259→18.761→19.791%. source-derived 생성점 대조1229개에서 기준점 의존 확인.|전파 endpoint 자체가 fit으로 정해져 77.3%를 독립 인과기여율로 해석 못함. GEN 기반 재평가점은 DATA용 처치 아님.|truth_geometry, geometry_selection, embedded_truth 보고서|
|INV-11|vertex truth/오차 pilot64개|embedding reference 검증 후 64대표 pull 대조. 큰 종방향 잔차/과도한 pull 사례 존재; 모든64vertex covariance PD.|64개 층화 표본으로 전체 coverage 확정 못함. 원 official SIM truth 직접 대조와 source-derived 복원을 구분.|vertex_truth_validation_20260929/REPORT.md|
|INV-12|수렴·seed actual fitter 검사|1264×15=18,960 fits. 반복수100→300만 늘리면 동일. XYZ1μm는 모두 valid, PR pass 중앙 비율 불변; 예외/seed 의존·음수χ² 사례 확인.|수렴 강화만으로 중앙 core 해결 안 됨. valid만으로 수치/물리 정상 보장 불가.|fit_stability_20260929/REPORT.md|
|INV-13|실제 fitter 입력 위치·covariance 개입|1229×23=28,267 fits, baseline exact. PR pass D0 방위각 offset 제거 시 FWHM .610→.746MeV, 중앙 비율 −3.105pp [−5.600,−.711].|인위적 원인 분리이며 DATA 처치 아님. D0 covariance×10에 따른 nominal broadening은 주로 D0 질량 이동으로 narrow core 제거와 다름.|bdt_refit_causality_20260929/refit_input_interventions/REPORT.md|
|INV-14|실제 ONNX 입력 기여/조건부 비교|1281 score 복원, 561tree ONNX/native 대조. PR pass−fail score 기여는 pointing·length/significance가 주도. Pointing 균형127pair에서 before→after FWHM interaction 점추정 −.613→−.032MeV.|score 기여 ≠ 질량 원인 분율. 조건부 subset/CI·공통 support 한계. 재훈련/입력 제거의 효과를 확정하지 않음.|bdt_refit_causality_20260929/bdt_input_attribution/REPORT.md|
|INV-15|GEN mass와 mass→DCA closure|6159 전 cos 고정후보. 목표 PR pass GEN 절대잔차68%반경 .674→.773MeV, 차이 +.099 [.014,.154]. A/B split closure는 before/after 모두 fold별 yield bias 남음.|narrow FWHM=정확도 개선, no-refit=추출 정상이라는 결론 모두 불가. 실제 DATA/전체효율/ρ00 bias 아직 없음.|bdt_refit_causality_20260929/refit_mass_decision/REPORT.md|
|INV-16|production 전체 static audit와 수정 기록|EP/event selection/fit/ntuple/GEN/MVA/운영 위험, code 위치와 설정 차이 전부 수록. DIAG 44개 제안 전체 명세 포함.|확인된 코드 결함, 정의 차이, 실제 발생률 미측정 위험을 분리. 문서 갱신 자체는 새 코드 수정 아님.|production_readonly_audit_20260929/REPORT.md, BRANCH_SCHEMA.md 및 Fix01–03 기록|
|INV-17|기존 vertex pilot의 전체 가용 표본 확장|source-derived 기준점과 common-vertex 상태가 있는 1,229후보를 연결. PR pass 491개의 비행방향 pull SD 1.379 [1.087,1.664]; NPR pass 1후보는 valid fit인데 vertex covariance indefinite. 기존 pilot 62개 재현·상위 9개 원본 modal 기준점 직접 대조.|원래 official SimVertex 직접 truth·전체 reference screen·누락 NPR 52개와 DATA 성능은 미검증. 중앙 Δm peak 원인/처치 확정 아님.|vertex_truth_validation_20260929/full_sample_20260929/REPORT.md|

### EP 기준 5개 cos 비교 — 완료 결과

이 표는 9/19 PR pT4 2,288개, 기존 nominal EP cos·weight·후보 집합 고정 비교다. 뒤의 tracker-axis BDT 표본과 합치지 않는다. 중앙 비율 변화는 **percentage points**다.

|고정 abs(cosθ*EP) 구간|N|fit 전 중앙 비율|일관된 fitted children 중앙 비율|변화 pp|
|---|---:|---:|---:|---:|
|0–0.2|456|16.111%|15.007%|−1.103 ± 1.695|
|0.2–0.4|462|16.449%|18.194%|+1.746 ± 1.959|
|0.4–0.6|424|14.051%|16.670%|+2.618 ± 1.885|
|0.6–0.8|501|15.152%|19.999%|+4.846 ± 1.861|
|0.8–1.0|445|14.479%|17.456%|+2.977 ± 2.137|

목표 구간 변화와 나머지 네 구간 통합 변화의 차이는 +3.321±2.094pp다. 목표 구간 PSD171개는14.786→18.357→18.357%, indefinite330개는15.342→20.849→21.156%다. Vertex fit 중앙 유입56/유출32, fitted→nominal 추가 유입1/유출0. 전체 68%폭1.299→1.445→1.446MeV. 따라서 covariance 이상만으로 중앙 증가를 설명하거나 전체 resolution 개선으로 부르지 않는다.

### BDT·기하·수치 안정성에서 확인된 연결

실제 ONNX는 547,090bytes, SHA256 `cd431de656a401610062481e5b6a88f20b534231836f631e9cf49d2461bbc024`다. 561tree 전체를 사용하며 best_iteration=510의 prefix만으로 바꾸지 않았다. 1281후보의 저장 score 복원 차이0, native/ONNX 최대 차이1.073e−6이고 threshold 분류가 같다. 입력벡터24개는 최대 float차이4.768e−7이며 모든 입력이 bit-identical이라고 쓰지 않는다. length significance는 원래 runtime의 sigma>0 분기를 따라 NaN/invalid 경우0이 들어가는 정의를 재현했다. 원문에 20개 입력의 전체 매핑과 NaN 사례가 있다.

PR pass−fail logit 차이4.705 중 pointing2.061, length/significance1.846이다. 합계 약83%는 **score 차이 기여**이며 mass 집중의 인과기여율이 아니다. Pointing 공통지지영역127쌍은 pass weight 약26%만 포함한다. 전체 interaction −.613MeV의95%CI[−.934,+.542], pointing-matched −.032의CI[−.861,+.734]로 유일 원인을 확정하지 않는다. Daughter kinematics matched218쌍에서도 잔여 차이와 넓은 CI가 남는다.

실제 위치 입력 개입은 score·입력 momentum·weight를 고정했다. D0 방위각 offset 제거가 만드는 Δm 이동의 weighted RMS는 전체 .07244MeV, slow 방향 .06810, slow momentum 크기 .00017, D0 질량 .02352다. 이는 D0 위치/기하→common vertex→slow 상대방향→Δm 경로의 직접 반응이다. 상관된 RMS를 더하거나 인과비율로 바꾸지 않는다. 두 수직 offset 모두 제거하면 D0 질량 기여도 커진다. 이 조작은 nonprompt 생성점을 PV로 강제하는 production 변경이 아니다.

수렴 검사에서는 PR pass491 중앙 비율19.791067%가 XYZ1μm에서도 그대로이고 width68은1.448572→1.448741MeV다. 대표2016(PR fail/slow PSD)은 default1iteration에서 강화 후8iterations, vertex8.457mm·Δm .219953MeV 이동한다. 대표9209(NPR pass/indefinite)는 seed+5mm 대조에서 valid이나 χ²=−12.734, Δm+3.027527MeV다. 이런 예외를 숨기거나 대체 nominal로 채택하지 않았다. 대표3148/10270의 큰 vertex pull도 단순 수렴 강화로 해결되지 않았다. 전체 event key·조건·표는 원문에 있다.

### 최신 GEN mass/추출 검증의 판단 범위

최신 mass decision은 6159고정후보, shared140–153MeV window6151, GEN mother chain·p4 대조를 사용한다. 목표 PR pass491의 before→nominal FWHM1.275→.610MeV와 GEN 잔차 width68 1.305→1.450MeV는 서로 다른 진술이다. GEN 절대잔차 68%반경 증가는 +.099MeV [+.014,+.154]이며 pointwise interval이다. 여러 cos/여러 지표의 다중검정을 보정한 확증으로 부르지 않는다.

Mass→DCA 검사는 실제 분석의 mass/DCA 코드를 사용한 A/B event-hash split이다. 880mass fits/88chains/44paired toys는 모두 status0/covQual3이나, shape fit60개 중1개 비정상·23개 boundary가 있다. Full-exposure empirical A에서는 yield bias before−14.76%/after−11.63%, B에서는+16.05%/+25.68%다. 따라서 한쪽 상태를 고르면 closure가 자동으로 해결되는 결과가 아니다. 10% exposure ensemble은 작고 full exposure는 fold당1toy이며, DCA<.08cm의 frozen sample에서 scale<1 유입이 빠지는 제한이 있다. 실제 DATA background/효율·cos/BDT 재선택과 최종ρ00 bias는 검증하지 않았다.

### 이 문서에서 누락하지 않은 미완료 항목

- 과거 data 44branch 명세는 **미구현 제안의 기록**. DStar41 + D0/EP/EventInfo event64각1이라는 당시 설계이며, 2026-09-30 EV-03의 32-bit 유지 결정으로 diagEvent64 추가는 채택하지 않는다. 기존 EOS MC 진단299branch와 다른 설계다. 실패 pair 추가 출력은 FIT-06 결정으로 미채택. Cross-daughter covariance/전파 σΔm, MC GEN 확장 및 성공 후보의 필요한 field 범위는 별도 판단 대상이며 구현 완료가 아니다.
- EP p/m 수정 build·event 처리, data/MC 새 filter 실제 cutflow·GEN 분모 영향, candidate cap·missing collection 발생률, EP DB/IOV 보정 적합성은 완료로 쓰지 않았다. EV-03은 기존 32-bit 유지로 결정했으며, 제한된 107파일 검사에서 overflow 0을 확인했지만 전체 32 stream의 overflow 부재 검증을 완료한 것은 아니다.
- 공식 문제 후보의 independent hit-based truth/original RECO, 전체 covariance coverage, 모든 event/후보에 대한 fitting 신뢰성, DATA/MC 효율을 포함한 대체 fit validation은 남아 있다.
- 전체 SOURCE/PLAN/STATUS 본문에서 과거에 “pending”, “MC untouched”, “no submissions”로 적힌 부분은 해당 날짜·단계의 기록이다. 위 현재 상태 및 후속 결과가 우선한다. 새 계산을 완료했다고 만들어 채우지 않았다.


<a id="source-catalogue"></a>

## 전체 수록 목록

범위: `DStarRefitDiagnostics_20260918`의 조사 Markdown 전체(실행 환경/소스 복사본/이전 문서 백업 제외) 및 Fix01–03의 실제 diff·검증 기록. 총 **58개 조사·정의·계획 문서 + 11개 수정/검증 기록**이다. 원문에 남은 상대 파일명은 원본 문서 디렉터리를 기준으로 읽는다. Markdown 상대 링크·그림은 원본 위치의 절대 경로로 연결했다. 원문 내용은 생략하지 않았으며 제목 깊이와 링크 위치만 정리했다.

|본문 이동|구분|원본|
|---|---|---|
|[SRC-01](#src-01)|정의·설계·재현 자료 (각 본문의 구현 상태 참조)|[README.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/README.md)|
|[SRC-02](#src-02)|조사 보고서·해석 (표본·시점별 결과)|[REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/REPORT.md)|
|[SRC-03](#src-03)|해당 시점 상태·제출 기록 (현재 queue 조회 아님)|[SUBMISSION_REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/SUBMISSION_REPORT.md)|
|[SRC-04](#src-04)|조사 보고서·해석 (표본·시점별 결과)|[audit_covariance/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/audit_covariance/REPORT.md)|
|[SRC-05](#src-05)|계획·제안 원문 (완료의 증거 아님)|[bdt_refit_causality_20260929/COVARIANCE_AUDIT_PLAN.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/COVARIANCE_AUDIT_PLAN.md)|
|[SRC-06](#src-06)|계획·제안 원문 (완료의 증거 아님)|[bdt_refit_causality_20260929/EXISTING_RECO_PLAN.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/EXISTING_RECO_PLAN.md)|
|[SRC-07](#src-07)|조사 보고서·해석 (표본·시점별 결과)|[bdt_refit_causality_20260929/EXISTING_RECO_REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/EXISTING_RECO_REPORT.md)|
|[SRC-08](#src-08)|계획·제안 원문 (완료의 증거 아님)|[bdt_refit_causality_20260929/FOLLOWUP_PLAN.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/FOLLOWUP_PLAN.md)|
|[SRC-09](#src-09)|조사 보고서·해석 (표본·시점별 결과)|[bdt_refit_causality_20260929/FOLLOWUP_REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/FOLLOWUP_REPORT.md)|
|[SRC-10](#src-10)|계획·제안 원문 (완료의 증거 아님)|[bdt_refit_causality_20260929/LARGE_WORK_PROPOSAL.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/LARGE_WORK_PROPOSAL.md)|
|[SRC-11](#src-11)|조사 보고서·해석 (표본·시점별 결과)|[bdt_refit_causality_20260929/PILOT_COVARIANCE_REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/PILOT_COVARIANCE_REPORT.md)|
|[SRC-12](#src-12)|조사 보고서·해석 (표본·시점별 결과)|[bdt_refit_causality_20260929/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/REPORT.md)|
|[SRC-13](#src-13)|해당 시점 상태·제출 기록 (현재 queue 조회 아님)|[bdt_refit_causality_20260929/STATUS.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/STATUS.md)|
|[SRC-14](#src-14)|계획·제안 원문 (완료의 증거 아님)|[bdt_refit_causality_20260929/bdt_input_attribution/PLAN.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/bdt_input_attribution/PLAN.md)|
|[SRC-15](#src-15)|조사 보고서·해석 (표본·시점별 결과)|[bdt_refit_causality_20260929/bdt_input_attribution/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/bdt_input_attribution/REPORT.md)|
|[SRC-16](#src-16)|계획·제안 원문 (완료의 증거 아님)|[bdt_refit_causality_20260929/embedded_truth/PLAN.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/embedded_truth/PLAN.md)|
|[SRC-17](#src-17)|정의·설계·재현 자료 (각 본문의 구현 상태 참조)|[bdt_refit_causality_20260929/embedded_truth/REFERENCE.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/embedded_truth/REFERENCE.md)|
|[SRC-18](#src-18)|조사 보고서·해석 (표본·시점별 결과)|[bdt_refit_causality_20260929/embedded_truth/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/embedded_truth/REPORT.md)|
|[SRC-19](#src-19)|계획·제안 원문 (완료의 증거 아님)|[bdt_refit_causality_20260929/geometry_selection/PLAN.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/geometry_selection/PLAN.md)|
|[SRC-20](#src-20)|조사 보고서·해석 (표본·시점별 결과)|[bdt_refit_causality_20260929/geometry_selection/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/geometry_selection/REPORT.md)|
|[SRC-21](#src-21)|계획·제안 원문 (완료의 증거 아님)|[bdt_refit_causality_20260929/matched_shape_bootstrap/PLAN.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/matched_shape_bootstrap/PLAN.md)|
|[SRC-22](#src-22)|조사 보고서·해석 (표본·시점별 결과)|[bdt_refit_causality_20260929/matched_shape_bootstrap/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/matched_shape_bootstrap/REPORT.md)|
|[SRC-23](#src-23)|계획·제안 원문 (완료의 증거 아님)|[bdt_refit_causality_20260929/official_posdef_108/PLAN.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/official_posdef_108/PLAN.md)|
|[SRC-24](#src-24)|조사 보고서·해석 (표본·시점별 결과)|[bdt_refit_causality_20260929/official_posdef_108/REFIT_BEFORE_AFTER.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/official_posdef_108/REFIT_BEFORE_AFTER.md)|
|[SRC-25](#src-25)|조사 보고서·해석 (표본·시점별 결과)|[bdt_refit_causality_20260929/official_posdef_108/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/official_posdef_108/REPORT.md)|
|[SRC-26](#src-26)|계획·제안 원문 (완료의 증거 아님)|[bdt_refit_causality_20260929/refit_input_interventions/PLAN.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/refit_input_interventions/PLAN.md)|
|[SRC-27](#src-27)|조사 보고서·해석 (표본·시점별 결과)|[bdt_refit_causality_20260929/refit_input_interventions/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/refit_input_interventions/REPORT.md)|
|[SRC-28](#src-28)|계획·제안 원문 (완료의 증거 아님)|[bdt_refit_causality_20260929/refit_mass_decision/PLAN.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/refit_mass_decision/PLAN.md)|
|[SRC-29](#src-29)|조사 보고서·해석 (표본·시점별 결과)|[bdt_refit_causality_20260929/refit_mass_decision/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/refit_mass_decision/REPORT.md)|
|[SRC-30](#src-30)|계획·제안 원문 (완료의 증거 아님)|[bdt_refit_causality_20260929/target_bin_posdef/PLAN.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/target_bin_posdef/PLAN.md)|
|[SRC-31](#src-31)|조사 보고서·해석 (표본·시점별 결과)|[bdt_refit_causality_20260929/target_bin_posdef/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/target_bin_posdef/REPORT.md)|
|[SRC-32](#src-32)|계획·제안 원문 (완료의 증거 아님)|[bdt_refit_causality_20260929/truth_geometry/PLAN.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/truth_geometry/PLAN.md)|
|[SRC-33](#src-33)|조사 보고서·해석 (표본·시점별 결과)|[bdt_refit_causality_20260929/truth_geometry/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/truth_geometry/REPORT.md)|
|[SRC-34](#src-34)|조사 보고서·해석 (표본·시점별 결과)|[cascade_method_review_20260929/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/cascade_method_review_20260929/REPORT.md)|
|[SRC-35](#src-35)|조사 보고서·해석 (표본·시점별 결과)|[causal_followup_20260919/DIRECTION_ABLATION.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/causal_followup_20260919/DIRECTION_ABLATION.md)|
|[SRC-36](#src-36)|조사 보고서·해석 (표본·시점별 결과)|[causal_followup_20260919/FULL_RECO_AVAILABILITY.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/causal_followup_20260919/FULL_RECO_AVAILABILITY.md)|
|[SRC-37](#src-37)|조사 보고서·해석 (표본·시점별 결과)|[causal_followup_20260919/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/causal_followup_20260919/REPORT.md)|
|[SRC-38](#src-38)|계획·제안 원문 (완료의 증거 아님)|[fit_stability_20260929/PLAN.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/fit_stability_20260929/PLAN.md)|
|[SRC-39](#src-39)|조사 보고서·해석 (표본·시점별 결과)|[fit_stability_20260929/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/fit_stability_20260929/REPORT.md)|
|[SRC-40](#src-40)|조사 보고서·해석 (표본·시점별 결과)|[handoff_replay_20260919/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/handoff_replay_20260919/REPORT.md)|
|[SRC-41](#src-41)|정의·설계·재현 자료 (각 본문의 구현 상태 참조)|[handoff_replay_20260919/production_handoff/README.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/handoff_replay_20260919/production_handoff/README.md)|
|[SRC-42](#src-42)|조사 보고서·해석 (표본·시점별 결과)|[handoff_replay_20260919/production_handoff/production_agent_response_summary.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/handoff_replay_20260919/production_handoff/production_agent_response_summary.md)|
|[SRC-43](#src-43)|정의·설계·재현 자료 (각 본문의 구현 상태 참조)|[production_readonly_audit_20260929/BRANCH_SCHEMA.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/BRANCH_SCHEMA.md)|
|[SRC-44](#src-44)|조사 보고서·해석 (표본·시점별 결과)|[production_readonly_audit_20260929/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/REPORT.md)|
|[SRC-45](#src-45)|조사 보고서·해석 (표본·시점별 결과)|[production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/REPORT.md)|
|[SRC-46](#src-46)|정의·설계·재현 자료 (각 본문의 구현 상태 참조)|[slow_gen_response_20260919/DEFINITIONS.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/slow_gen_response_20260919/DEFINITIONS.md)|
|[SRC-47](#src-47)|조사 보고서·해석 (표본·시점별 결과)|[slow_gen_response_20260919/INTERPRETATION.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/slow_gen_response_20260919/INTERPRETATION.md)|
|[SRC-48](#src-48)|조사 보고서·해석 (표본·시점별 결과)|[slow_gen_response_20260919/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/slow_gen_response_20260919/REPORT.md)|
|[SRC-49](#src-49)|정의·설계·재현 자료 (각 본문의 구현 상태 참조)|[three_stage_pr4_20260919/DEFINITIONS.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/three_stage_pr4_20260919/DEFINITIONS.md)|
|[SRC-50](#src-50)|조사 보고서·해석 (표본·시점별 결과)|[three_stage_pr4_20260919/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/three_stage_pr4_20260919/REPORT.md)|
|[SRC-51](#src-51)|정의·설계·재현 자료 (각 본문의 구현 상태 참조)|[three_stage_pr4_all_cos_20260919/DEFINITIONS.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/three_stage_pr4_all_cos_20260919/DEFINITIONS.md)|
|[SRC-52](#src-52)|조사 보고서·해석 (표본·시점별 결과)|[three_stage_pr4_all_cos_20260919/INTERPRETATION.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/three_stage_pr4_all_cos_20260919/INTERPRETATION.md)|
|[SRC-53](#src-53)|계획·제안 원문 (완료의 증거 아님)|[three_stage_pr4_all_cos_20260919/NEXT_STEPS.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/three_stage_pr4_all_cos_20260919/NEXT_STEPS.md)|
|[SRC-54](#src-54)|조사 보고서·해석 (표본·시점별 결과)|[three_stage_pr4_all_cos_20260919/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/three_stage_pr4_all_cos_20260919/REPORT.md)|
|[SRC-55](#src-55)|해당 시점 상태·제출 기록 (현재 queue 조회 아님)|[three_stage_pr4_all_cos_20260919/STATUS.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/three_stage_pr4_all_cos_20260919/STATUS.md)|
|[SRC-56](#src-56)|정의·설계·재현 자료 (각 본문의 구현 상태 참조)|[three_stage_pr4_all_cos_20260919/production_handoff_all_cos/README.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/three_stage_pr4_all_cos_20260919/production_handoff_all_cos/README.md)|
|[SRC-57](#src-57)|조사 보고서·해석 (표본·시점별 결과)|[vertex_truth_validation_20260929/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/vertex_truth_validation_20260929/REPORT.md)|
|[SRC-58](#src-58)|조사 보고서·해석 (표본·시점별 결과)|[vertex_truth_validation_20260929/full_sample_20260929/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/vertex_truth_validation_20260929/full_sample_20260929/REPORT.md)|
|[EVD-01](#evd-01)|실제 수정 diff·정적/설정 검증·코드 이력|[production_readonly_audit_20260929/fix01_tracker_ep_pm/tracker_ep_pm.diff](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix01_tracker_ep_pm/tracker_ep_pm.diff)|
|[EVD-02](#evd-02)|실제 수정 diff·정적/설정 검증·코드 이력|[production_readonly_audit_20260929/fix01_tracker_ep_pm/verification.json](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix01_tracker_ep_pm/verification.json)|
|[EVD-03](#evd-03)|실제 수정 diff·정적/설정 검증·코드 이력|[production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/production_offline_selection.diff](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/production_offline_selection.diff)|
|[EVD-04](#evd-04)|실제 수정 diff·정적/설정 검증·코드 이력|[production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/verification.json](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/verification.json)|
|[EVD-05](#evd-05)|실제 수정 diff·정적/설정 검증·코드 이력|[production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/canonical_validation.json](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/canonical_validation.json)|
|[EVD-06](#evd-06)|실제 수정 diff·정적/설정 검증·코드 이력|[production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/condor_validation.json](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/condor_validation.json)|
|[EVD-07](#evd-07)|실제 수정 diff·정적/설정 검증·코드 이력|[production_readonly_audit_20260929/fix03_mc_offline_selection/mc_offline_selection.diff](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix03_mc_offline_selection/mc_offline_selection.diff)|
|[EVD-08](#evd-08)|실제 수정 diff·정적/설정 검증·코드 이력|[production_readonly_audit_20260929/fix03_mc_offline_selection/canonical_validation.json](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix03_mc_offline_selection/canonical_validation.json)|
|[EVD-09](#evd-09)|실제 수정 diff·정적/설정 검증·코드 이력|[production_readonly_audit_20260929/fix03_mc_offline_selection/condor_validation.json](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix03_mc_offline_selection/condor_validation.json)|
|[EVD-10](#evd-10)|실제 수정 diff·정적/설정 검증·코드 이력|[production_readonly_audit_20260929/document_reconciliation/verification.json](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/document_reconciliation/verification.json)|
|[EVD-11](#evd-11)|실제 수정 diff·정적/설정 검증·코드 이력|[production_readonly_audit_20260929/document_reconciliation/FIT05_blame.txt](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/document_reconciliation/FIT05_blame.txt)|

<!-- full-investigation-corpus:start -->

<a id="src-01"></a>

## SRC-01 — README.md

**기록 구분:** 정의·설계·재현 자료 (각 본문의 구현 상태 참조). **원문:** [README.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/README.md). **SHA256:** `92a12ffdb254f733a6f94492e593681b3c6327f0009f5f0fdf1bc31d4427691b`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-01:start -->
### D* refit diagnostics, official PAT6SigGen input — 2026-09-18

Status: all three target-sample before/after checks passed and three CRAB submissions accepted. See SUBMISSION_REPORT.md.

Independent EOS workspace. The 2026-09-09 frozen official source agrees byte-for-byte with the AFS D0Fitter, DStarFitter, PAT6 and PATEventPlaneTrack source. The AFS producer/analyzer binary SHA256 values still agree with the official CRAB sandbox identity verified on 2026-09-12. This workspace deliberately uses that production baseline, rather than importing later EOS selection, XY-DCA, event-plane and matching changes.

#### Inputs and output

`provenance/submission_manifest.json` and `inputs/*_lfns.txt` record the exact previously processed official MiniAOD files: PR pThat2/pT4 934, NPR pThat2/pT4 774, PR pT0 444. The NPR dataset contains one additional file not processed by the original campaign; it is not added to this comparison. Source is `DStarNormalization_20260916/full_audit/tasks.json` and `input_scope.json`, not D0 MC.

`configs/official_*.py` are the three original submitted CRAB PSetDump.py files. `configs/production_*.py` only enable additive diagnostics on D0/D* fitters and the existing D* PAT6 analyzer, add an input-file tracking service, and change runtime/output controls, and replace the old source placeholder with an official MiniAOD example (CRAB assigns the actual job LFNs). All original paths, selections, matching and calibration modules remain. No raw/fitChildren alternative producer, selection variation, extra PAT tree, cos cut or post-refit deltaM cut is added. The original prefit deltaM<=0.160 and all original reconstruction selections remain.

D* diagnostics are branches on `dStarana_mc/PATCompositeNtuple`. Original D0, EventInfo and event-plane trees are retained. There are zero extra TTrees; `RefitDiagnosticSchema` is TNamed metadata. New output target is `T3_KR_KNU`, `/store/user/junseok/DStarRefitDiagnostics/20260918`, with unique requests. Existing production is not overwritten.

#### Stage contract

1. Original K/pi/slow values: original TrackRefs at their reference points. K/pi labels are reconstructed mass hypotheses (higher daughter mass is K). ProductID plus key disambiguates track identity.
2. First D0 fit: K/pi vertex fit in D0Fitter. Store both daughters and parent fit-state parameters/covariance, vertex/chi2/ndf and input mass hypotheses/sigmas. Existing stored D0 p4 is the historical float daughter-sum convention, and is stored separately as `diagD0OriginalP4`.
3. Internal D0 fit: original daughter TrackRefs are fit again in DStarFitter. Capture parent, daughter states and vertex *before* passing the composite into the final D* fit; restore the tree pointer to its top. No added fit is performed.
4. D* fit: input D0 composite plus slow pion are vertex fit. Store D0 and slow-pion child states, D* parent and common vertex. K/pi are not independently reoptimized at this stage (`diagKRefitAtDStar=diagPiRefitAtDStar=0`); their latest independent states are InternalD0. Slow-pion flag is 1.

No explicit PDG D0 mass or PV constraint is applied. The mass hypotheses and their input uncertainties are stored, and are not described as a PDG D0 invariant-mass constraint. Vertex fits are applied. Surviving candidates have valid fit status; failed candidates are not newly introduced into the original selected collection.

#### DeltaM contract

Let O = original stored D0 p4 after its first fit, R = original slow-pion TrackRef p4 with the configured pion mass, F = D0 child state after D* fit (direct double KinematicParameters), S = slow-pion child state after D* fit. L0 and Lpi are the original legacy float/global-vector daughter p4s; L0 uses refitted D0 momentum but original D0 mass in its energy.

- `integrityDmLegacy` = existing candidate mass - M(O).
- `integrityDmRawBoth` = M(O+R)-M(O).
- `integrityDmRawPiOnly` = M(L0+R)-M(O).
- `integrityDmRawD0Only` = M(O+Lpi)-M(O).
- `integrityDmFitChildren` = M(float/global-vector state-mass children sum) - scalar fitted D0 mass. Retains the older diagnostic arithmetic definition, saved now as double.
- `deltaMOriginal` = M(O+R)-M(O).
- `deltaMRefit` = M(F+S)-M(F), using direct double state parameters.
- `deltaMD0` = M(F)-M(O).
- `deltaMDStar` = M(F+S)-M(O+R).
- `deltaDeltaM` = deltaMRefit-deltaMOriginal = deltaMDStar-deltaMD0.
- `diagDeltaMRawTracks` additionally computes deltaM using all three original TrackRefs before the first D0 fit.

Definitions are implemented in `VertexCompositeProducer/src/DStarFitter.cc` (legacy and float-fit intermediates) and `VertexCompositeAnalyzer/plugins/PAT6RefitDiagnostics.icc` (same-row p4 combinations). The stored legacy mass is not equated to the mass directly returned by the fitted D* state.

#### GEN and identity

Existing matching and `matchGEN` are unmodified. The new GEN fields are filled inside the original accepted matching branch. Preserve prunedGenParticles indices of D*, D0, true GEN K/pi/slow, PDG/status/charge/p4/collisionId, all direct mothers and ancestry reached along all mother edges. Unmatched candidates retain index -1, Matched=0 and NaN kinematic values.

`diagGenK/Pi` refer to GEN species and `diagK/Pi` to reconstructed mass hypotheses. For swapped candidates the species-wise DeltaR is not the accepted association. `diagMatchedKTrackGenIndex`, `diagMatchedPiTrackGenIndex`, `diagMatchedSlowTrackGenIndex` and corresponding DeltaR record the actual charge/cone-compatible permutation. No independent hit-based track truth is available in this MiniAOD workflow (`diagIndependentTrackTruthAvailable=0`). GEN collision origin of the matched particle does not prove the detector track's physical origin.

Keep old RunNb/LSNb/EventNb; add unsigned 64-bit `diagEvent64` without changing old branch type. `diagInputFile` comes from the framework's input-file-open signal. Combine input LFN/file identity, run/lumi/event, candidate index, ProductID and track keys for duplicate checks and external weight joins. Existing event-plane Q/angles and original calibration chain remain untouched.

#### Covariance

Original track covariance: row-major 5x5 in `(q/p,lambda,phi,dxy,dsz)`.
Fit covariance: row-major 7x7 in `(x,y,z,px,py,pz,m)` for individual states. State rows are `[px,py,pz,E,charge,ptError,mass,x,y,z]`. Fit pT error is propagated from px/py covariance including Cxy. Vertex rows are `[valid,chi2,ndf,x,y,z,cov3x3]`.

Cross-daughter covariance is not exposed by the stored KinematicTree states. It has not been added to the CMS fitter internals here. `diagCrossCovarianceAvailable=0`, `diagSigmaDeltaMAvailable=0`, `diagSigmaDeltaM=NaN`. No independence assumption is silently used to manufacture sigmaDeltaM. In-memory vector payloads are transient PAT userData and are written to ROOT; no diagnostic EDM output is scheduled.

#### Reproduction and validation

- Environment: `scripts/env.sh` under `cmssw-el8`, CMSSW_13_2_11, el8_amd64_gcc11; all working files under this EOS directory.
- `scripts/instrument.py`: asserted mechanical edits against frozen source; final `.icc` is stored in the source snapshot.
- `scripts/audit_config.py`: compares complete expanded original/new PSets after removing only diagnostic/runtime changes; also verifies paths/schedule.
- `scripts/validate.py`: checks same TTrees, every old branch, candidate counts/order, new array shapes, finite state/covariance values, pT error propagation, GEN association flags and deltaM identities.
- `logs/`, `validation/`, `provenance/`: logs, ROOT and input/config/source identity.

A build success or cached pThat10 smoke test alone is not a successful PR/NPR pT4/pT0 production test. Target-sample tests and submission state are recorded explicitly in the final report.

#### Full-target covariance observation

PR pT4 full-file validation contains two first-D0 parent states with negative propagated pT variance (-0.0008444584681 and -0.0021088355130 GeV^2). Their pT errors are NaN as required by sqrt(negative variance). Individual covariance matrices and all other state parameters are finite. These are preserved, with no clipping, fallback, or candidate rejection. The initial validator incorrectly assumed every propagated error must be finite; it now verifies the numerical propagation including exactly the expected NaNs. Every original branch in all five trees remains bitwise identical. See validation/nonfinite_covariance_audit.json and logs/target_validation.log.

The same two candidate rows (events 320539796/index 3 and 320565126/index 5 in the PR pT4 validation file) also have negative pT variance in the internal D0, final D0 and D* parent state covariances. Individual daughter pT-error values in these tests are finite. This observation is not a claim about the origin of the central mass-bin structure.
<!-- source:SRC-01:end -->

<a id="src-02"></a>

## SRC-02 — REPORT.md

**기록 구분:** 조사 보고서·해석 (표본·시점별 결과). **원문:** [REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/REPORT.md). **SHA256:** `ec452bec9f2a2af438bb85ba2937ca53465920f2bb0a7c599d3243a35cd3d57d`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-02:start -->
**UPDATE: three target tests passed and three CRAB submissions accepted. See SUBMISSION_REPORT.md. The pending-proxy statements below are historical.**

### Implementation and validation status — 2026-09-18

**Code built and available; target NPR pT4 test passed. PR pT4/PR pT0 tests and all three CRAB submissions are pending CMS proxy renewal and successful remote input access. No CRAB task has been submitted by this work.**

#### Implemented

- Independent EOS CMSSW_13_2_11 build against the exact official production baseline. AFS production source/binaries and the previous EOS integrity runtime were not modified.
- Additive D0/DStar fitter payloads and existing D* PAT6 branches. No new fitter invocation, selection change, matching policy, compact variation or TTree.
- Candidate/run/lumi/64-bit event/input-file/track ProductID+key identity.
- Original track, first D0 fit, internal D0 re-fit and final D0+slow-pion fit states. Momentum, charge, pT error, individual covariance, vertex/status/chi2/ndf and mass hypotheses/sigmas.
- Seven requested deltaM definitions plus explicit deltaMDStar, deltaMD0, deltaDeltaM and fully raw-three-track deltaM.
- Original matchGEN and selected GEN decay association preserved; new GEN indices, PDG/status/p4/charge/collision origin, mothers/ancestors and matching DeltaR including the accepted swapped-track permutation.
- Original event-plane/calibration/EventInfo trees and all their old branches preserved.
- Cross-daughter covariance and correlated sigma(deltaM) remain unavailable and are explicitly flagged, not approximated as independent.

D* tree has its original 249 branches plus 299 additive diagnostic branches. Other TTrees have no new branches. There are still exactly the original five TTrees: D0 PAT, D* PAT, EventInfo, EventPlane, GenEventPlane. Additional schema information is a TNamed, not a TTree.

#### Executed validation

Each row compares the same MiniAOD input with the original official AFS binary against this new EOS build. Exactly 100 input events per row.

| Input | Saved entries in each original tree | D* candidates | Matched D* | All original branches |
|---|---:|---:|---:|---|
| Cached official prompt pThat10/pT20 smoke | 76 | 195 | 42 | exactly identical |
| Cached official nonprompt pThat10/pT20 smoke | 78 | 164 | 23 | exactly identical |
| **Target official nonprompt pThat2/pT4** | **65** | **147** | **2** | **exactly identical** |

All old branches in all five trees were compared, including mass, momentum, matching, candidate counts/order and event-plane/calibration values. Zero old-branch differences. The pThat10 samples are additional implementation checks; they do not substitute for the outstanding PR pT4 and PR pT0 tests.

New-branch checks passed: candidate-aligned lengths, nonempty input-file names, 64-bit event identity, finite individual states/covariances, pT-error propagation from covariance, correct GEN PDG/index/matching flags, refit-stage flags, and deltaMDStar-deltaMD0=deltaMRefit-deltaMOriginal. SigmaDeltaM is intentionally NaN with availability=0.

Evidence:
- `validation/comparison_pr_smoke_npr_smoke.json`
- `validation/comparison_npr4.json`
- `validation/*_official.root`, `validation/*_diagnostic.root`
- `logs/build_final.log`, `logs/*smoke*.log`, `logs/npr4_official_local.log`, `logs/npr4_diagnostic.log`
- `provenance/configuration_audit.json`: three full PSet comparisons; same paths/schedule and physics settings after removing storage/runtime and source-placeholder overrides.

Target NPR file LFN:
`/store/mc/HINPbPbSpring23MiniAOD/nonpromptDStarToD0PiToKPiPi_pThat-2_pT-4_TuneCP5_5p36TeV_pythia8-evtgen/MINIAODSIM/132X_mcRun3_2023_realistic_HI_v9-v2/2820000/0dc2b6cb-3d9c-484f-8fad-8ab2bab67c65.root`

The original processing inventory records 170 events in this file. A local copy is `inputs/npr4_miniaod.root`; the test reads its first 100 events. Direct old-CMSSW remote reading failed with TLS error, while host xrdcp successfully copied the file from CERN EOS; both records are retained.

#### Production preparation and remaining work

Prepared requests:
- DStarRefitDiag_pr4_PAT6SigGen_20260918_v1 — 934 previously processed official files
- DStarRefitDiag_npr4_PAT6SigGen_20260918_v1 — 774 previously processed official files
- DStarRefitDiag_pr0_PAT6SigGen_20260918_v1 — 444 previously processed official files

`configs/crab_*.py` uses the original official D* datasets and exact previous input-file lists, FileBased 1 file/job, 1 core, 3000 MB, max runtime 2750 min, T3_KR_KNU. New output base: `/store/user/junseok/DStarRefitDiagnostics/20260918`. The old NPR campaign did not process one of the dataset's 775 files; this diagnostic reproduces its 774-file scope.

The named proxy `/afs/cern.ch/user/j/junseok/private/DStarRefitDiagnostics_20260918.proxy` does not yet exist; default and existing private production proxies are expired. PR pT4 and PR pT0 global redirector transfers also failed (redirect limit reached). These samples have not been declared tested or submitted.

Renew with:
```bash
voms-proxy-init --voms cms --valid 192:00 --out /afs/cern.ch/user/j/junseok/private/DStarRefitDiagnostics_20260918.proxy
```

Then execute remaining target tests on the host:
```bash
python3 /eos/user/j/junseok/DStarRefitDiagnostics_20260918/scripts/run_remaining_tests.py
```
This fetches the recorded small official PR files, runs original/new builds, and compares PR pT4, NPR pT4 and PR pT0 together. Successful `validation/comparison_pr4_npr4_pr0.json` is required before submission.

Authorized submission command after those tests pass:
```bash
/cvmfs/cms.cern.ch/common/cmssw-el8 --command-to-run 'bash /eos/user/j/junseok/DStarRefitDiagnostics_20260918/scripts/env.sh bash scripts/crab_env.sh python3 scripts/submit.py'
```
The script preserves submission/status logs and request metadata and does not resubmit an existing project with `.requestcache`.

#### Source provenance

Content revision: `1e271d7ad110b552b0b5cc70204c53a6fd5e7c66a5c32187374fdebf7a2a0227`.

- `provenance/code_revision.json`: source content revision, original git HEAD, source/library/PSet/input checksums.
- `provenance/source_sha256.json`, `baseline_source_sha256.json`.
- `provenance/additive_diagnostics.patch`: complete source changes.
- `provenance/baseline_official_identity.json`: original AFS binary and frozen-source correspondence.
- `README.md`: stage, field, deltaM, GEN and covariance definitions.

Original code remains untouched. Changes to shared producer wrapper files were unnecessary; fitter interfaces/implementations, PAT6, and the file-name service implement the additions.
<!-- source:SRC-02:end -->

<a id="src-03"></a>

## SRC-03 — SUBMISSION_REPORT.md

**기록 구분:** 해당 시점 상태·제출 기록 (현재 queue 조회 아님). **원문:** [SUBMISSION_REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/SUBMISSION_REPORT.md). **SHA256:** `e0a4964ffbe2502cf6d4f6a16e12d56d826fe15150f35abf701a7346823e96b4`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-03:start -->
### CRAB submission completed — 2026-09-18

All three target-sample before/after comparisons passed. Every original branch in all five original TTrees is exactly identical, including candidate count/order, mass, momentum, matching and event-plane values. New diagnostic checks passed. Two PR pT4 candidates have negative propagated pT variance in the first-D0, internal-D0, post-DStar D0 and DStar parent covariances; those matrices and corresponding NaN pT errors are preserved. Validation explicitly checks that NaN occurs exactly when the stored covariance gives negative variance; no physics code, value clipping or selection was changed. The three CRAB submissions were accepted; worker completion is a separate state.

| Sample | Input files submitted | Validation D* candidates | Validation matched D* | CRAB server status |
|---|---:|---:|---:|---|
| pr4 | 934 | 3832 | 65 | WAITING on command SUBMIT |
| npr4 | 774 | 147 | 2 | WAITING on command SUBMIT |
| pr0 | 444 | 608 | 6 | WAITING on command SUBMIT |

Detailed validation: `validation/comparison_pr4_npr4_pr0.json`. PR pT4 and PR pT0 were tested on the complete selected small official files (1,160 and 212 input events respectively); NPR pT4 on 100 input events of its official file. Exact input LFNs/local paths: `provenance/target_test_inputs.json`. Production uses the original previously processed file lists in `inputs/*_lfns.txt`, not just these test files.

New output storage is `root://cluster142.knu.ac.kr//store/user/junseok/DStarRefitDiagnostics/20260918/`, under CRAB dataset/request/timestamp directories, filename `DStarRefitDiagnostics_<job>.root`. Existing production is not overwritten.

Existing five TTrees are retained; no extra tree. Diagnostics are on the existing D* PAT tree. Individual fit-state covariance is stored. Cross-daughter covariance and correlated sigma(deltaM) remain explicitly unavailable. Refer to README.md for all stage and GEN-association definitions.

Requests and server task IDs:

- DStarRefitDiag_pr4_PAT6SigGen_20260918_v1: `260918_061252:junseok_crab_DStarRefitDiag_pr4_PAT6SigGen_20260918_v1`

- DStarRefitDiag_npr4_PAT6SigGen_20260918_v1: `260918_061348:junseok_crab_DStarRefitDiag_npr4_PAT6SigGen_20260918_v1`

- DStarRefitDiag_pr0_PAT6SigGen_20260918_v1: `260918_061436:junseok_crab_DStarRefitDiag_pr0_PAT6SigGen_20260918_v1`

Evidence: `provenance/submission_verified.json`, `submission_results.json`, `submission_manifest.json`, `logs/submit_*.log`, `logs/status_*.log`, and `crab_projects/`. Code revision/library/PSet/input checksums remain in `provenance/code_revision.json`; complete patch in `provenance/additive_diagnostics.patch`. Full implementation and earlier test history: REPORT.md.
<!-- source:SRC-03:end -->

<a id="src-04"></a>

## SRC-04 — audit_covariance/REPORT.md

**기록 구분:** 조사 보고서·해석 (표본·시점별 결과). **원문:** [audit_covariance/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/audit_covariance/REPORT.md). **SHA256:** `c8bdac01c795e5ff44cf99051cd6222f8750a931ee63383a71a69755457dfcd8`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-04:start -->
### PR pT4 slow-pion covariance / refit 코드 및 대표 이벤트 대조

실제 제출 revision `1e271d7ad110b552b0b5cc70204c53a6fd5e7c66a5c32187374fdebf7a2a0227`, CMSSW_13_2_11, el8_amd64_gcc11 기준.
159개 production source, 3개 producer/analyzer library, 3개 PSet checksum을 provenance와 비교하여 모두 일치했다 (`provenance_check.json`). 기존 production 소스/PSet/library와 CRAB task는 변경하지 않았다. 별도 RefitAudit/Probe 검사 모듈을 추가하여 기존 PR4 PSet에서 3개 이벤트만 실행했다. covariance clipping, 후보 제거, constraint 변경, 전체 재생산은 하지 않았다.

#### 확인된 결론과 범위

3개 이벤트의 7개 surviving 후보에서 ROOT의 diagSlowOriginalCov와 packed pseudoTrack, unpacked track, transient track의 track covariance, initialFreeState curvilinear covariance가 원소별 정확히 일치했다 (최대 차이 0). 같은 EventSetup magnetic field로 factory 입력/출력을 재구성하여 D0와 D* fit을 반복했고, 최종 slow/D0/D* 상태도 기존 ROOT와 최대 차이 0이었다. 대표 matched 세 후보의 fit 직전 7x7 covariance에도 유의한 음의 고유값이 있다. 따라서 이 표본의 문제는 branch 순서/저장으로 만들어진 것이 아니다. MiniAOD packed candidate에서 unpack한 pseudoTrack 단계에 이미 존재한다.

이 표본은 보관된 검증 파일에서 고른 covariance 재현 사례이며, 요청한 pT7–10, |y|<0.3, 0–10%, |cos|0.6–0.8 후보 표본이 아니다. 4,364개 중 2,784개 및 최대 correlation 2.698 수치는 사용자가 제시한 결과로, 이 검사에서 그 전체 표본을 재집계하지 않았다. 중앙 비중 19.80→15.56%의 원인 또는 production 오류라고 확정하지 않는다. packed 저장 전 full reco covariance가 이미 문제였는지, packing/parameterization에서 생겼는지는 이 MiniAOD만으로 확정하지 않았다.

#### 1. 호출 경로와 constraint

1. 실제 path: [production_pr4.py:76604](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/configs/production_pr4.py:76604). `unpackedTracksAndVertices → generalD0CandidatesNew → generalDStarCandidatesNew → dStarana_mc`. Track 입력은 `unpackedTracksAndVertices`; `useRawDStarKinematics=False`, `VtxChiProbCut=0`, `rejectDuplicateSlowPion=False`: [production_pr4.py:33655](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/configs/production_pr4.py:33655).
2. `TrackAndVertexUnpacker::produce`: [TrackAndVertexUnpacker.cc:51](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/plugins/TrackAndVertexUnpacker.cc:51). hasTrackDetails인 경우 `cand.pseudoTrack()`의 momentum/referencePoint/charge/covariance를 복사(75–91). chi2/ndof와 일부 metadata만 재구성. 입력 순서는 packedPFCandidates, lostTracks, lostTracks:eleTracks: [production_pr4.py:63384](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/configs/production_pr4.py:63384).
3. `DStarProducer::produce → DStarFitter::fitAll`: [DStarProducer.cc:48](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarProducer.cc:48). slow track selection 후 `TransientTrack(*tmpRef,magField)` 생성 [DStarFitter.cc:393](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:393). 동일 인덱스의 TrackRef와 TransientTrack을 선택 [DStarFitter.cc:436](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:436).
4. D0의 원래 daughter bestTrack 두 개로 내부 D0 vertex fit을 다시 수행: [DStarFitter.cc:599](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:599). D0 top particle과 slow pion factory particle을 `dStarParticles`에 넣음 [DStarFitter.cc:633](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:633).
5. `KinematicParticleFactoryFromTransientTrack::particle`: [KinematicParticleFactoryFromTransientTrack.cc:15](/cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src/RecoVertex/KinematicFitPrimitives/src/KinematicParticleFactoryFromTransientTrack.cc:15) → `TransientTrackKinematicStateBuilder::operator()` [TransientTrackKinematicStateBuilder.cc:5](/cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src/RecoVertex/KinematicFitPrimitives/src/TransientTrackKinematicStateBuilder.cc:5) → impactPointState의 FreeTrajectoryState → KinematicState.
6. `KinematicParticleVertexFitter::fit`: [DStarFitter.cc:638](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:638) → [KinematicParticleVertexFitter.cc:48](/cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src/RecoVertex/KinematicFit/src/KinematicParticleVertexFitter.cc:48). default linearization point, SequentialVertexFitter + Kalman updator + smoother. maxDistance=0.01 cm, maxNbrOfIterations=100 (41–45). 초기 vertex seed covariance의 대각 10000 cm²는 수치적 seed이며 실제 PV 측정 constraint가 아니다.
7. child0=D0, child1=slow; fitted momentum 취득 [DStarFitter.cc:669](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:669). slow pion의 fitted pT/eta/phi는 이 최종 child momentum에서 정해진다. 후보 daughter에 fitted p4를 넣되 TrackRef는 original을 유지 [DStarFitter.cc:805](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:805).

명시적인 D0 PDG mass constraint, D* PDG mass constraint, PV constraint는 호출하지 않는다. 헤더 include만 된 TwoTrackMassKinematicConstraint 등은 실행 호출이 아니다. 공통 vertex fit은 적용한다. PV는 impact parameter/flight significance/pointing 계산 및 선택에서 사용하며, fitted state를 PV에 extrapolate하는 DCA 계산도 있다 [DStarFitter.cc:756](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:756); 이것은 PV에 재fit하는 constraint와 구분해야 한다.

질량 가설 및 오차(GeV): 최초 D0 K/pi는 σK=1.6e-5, σpi=3.5e-7 ([D0Fitter.cc:58](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/D0Fitter.cc:58), [D0Fitter.cc:457](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/D0Fitter.cc:457)). DStarFitter 내부 D0 재fit은 **두 daughter 모두 σ=1.6e-4**를 전달한다 [DStarFitter.cc:603](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:603). 변수명이 D0MassD0_sigma지만 D0 invariant mass constraint가 아니라 daughter factory의 mass uncertainty이다. slow pion은 mass=0.13957018(float), σ=3.5e-7 [DStarFitter.cc:63](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:63), [DStarFitter.cc:634](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:634). D* 2.010 및 ±0.22 mass window는 최종 selection [DStarFitter.cc:868](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:868)이며 mass constraint가 아니다.

#### 2. covariance 전달과 좌표 변환

- `diagSlowOriginalCov[event][candidate][5*i+j] = sp->track()->covariance(i,j)`; double 25개, row-major 5x5. [PAT6RefitDiagnostics.icc:15](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PAT6RefitDiagnostics.icc:15), [PAT6RefitDiagnostics.icc:73](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PAT6RefitDiagnostics.icc:73). 대칭 행렬의 packed 15-element memory를 그대로 복사한 것이 아니다.
- basis는 `(q/p,lambda,phi,dxy,dsz)`, 단위는 `(GeV^-1,rad,rad,cm,cm)`; covariance 원소는 해당 단위의 곱. lambda는 eta가 아니고 dsz는 dz가 아니다. [TrackBase.h:17](/cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src/DataFormats/TrackReco/interface/TrackBase.h:17).
- PackedCandidate `unpackCovariance()`는 parameterization에서 각 저장 성분을 복원하고 나머지를 0으로 초기화한다. `unpackTrk()`는 그 행렬로 reco::Track을 만든다. [PackedCandidate.cc:96](/cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src/DataFormats/PatCandidates/src/PackedCandidate.cc:96), [PackedCandidate.cc:216](/cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src/DataFormats/PatCandidates/src/PackedCandidate.cc:216). packed 필드명 dpt/deta/dz를 branch basis로 해석하면 안 된다.
- `TrackTransientTrack(track,field)`는 Track을 복사하고 initialFreeState 생성. [TrackTransientTrack.cc:36](/cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src/TrackingTools/TransientTrack/src/TrackTransientTrack.cc:36). `initialFreeState`는 track covariance를 CurvilinearTrajectoryError로 그대로 사용하고 position/momentum은 float GlobalPoint/GlobalVector로 변환한다. [TrajectoryStateTransform.cc:58](/cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src/TrackingTools/TrajectoryState/src/TrajectoryStateTransform.cc:58).
- `impactPointState()`는 initialFTS의 **자기 reference position**으로 transverse extrapolation을 수행한다. PV를 목표점으로 사용하지 않는다. [TrackTransientTrack.cc:193](/cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src/TrackingTools/TransientTrack/src/TrackTransientTrack.cc:193).
- Curvilinear 5x5 → Cartesian 6x6은 `J C J^T`: [FreeTrajectoryState.cc:31](/cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src/TrackingTools/TrajectoryState/src/FreeTrajectoryState.cc:31). KinematicState의 7x7은 이 6x6에 mass variance σm²를 추가하며 basis는 `(x,y,z,px,py,pz,m)`: [KinematicState.h:36](/cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src/RecoVertex/KinematicFitPrimitives/interface/KinematicState.h:36), [KinematicParametersError.h:25](/cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src/RecoVertex/KinematicFitPrimitives/interface/KinematicParametersError.h:25).
- 따라서 저장된 **5x5와 최종 fitter 입력 7x7은 같은 행렬 표현이 아니다**. 동일 원본 covariance에서 전파/변환된 관계다. 검사한 hasTrackDetails 경로에 clipping, correlation 정규화, SPD 보정, 대체행렬 삽입은 없다.
- 예외적으로 unpacker의 `recoverTracks=True`이며 hasTrackDetails가 없고 covarianceVersion>0이면 `1e12 I` covariance의 synthetic track을 만든다 (unpacker 101–112). 이번 대표 세 matched slow pion은 모두 hasTrackDetails=1, covarianceVersion=1, schema=520이므로 해당 경로가 아니다.

#### 3. 대표 이벤트 수치

Input LFN: `/store/mc/HINPbPbSpring23MiniAOD/promptDStarToD0PiToKPiPi_pThat-2_pT-4_TuneCP5_5p36TeV_pythia8-evtgen/MINIAODSIM/132X_mcRun3_2023_realistic_HI_v9-v2/120000/dd025f0a-36dc-46d1-8969-aa1fd1ca677a.root`.
기존 진단 ROOT: `validation/pr4_diagnostic.root`. run=1, lumi=4942, candidate index는 0-based. eig는 `Cij/sqrt(Cii*Cjj)`로 대각 정규화한 행렬 기준이다. 7x7의 약 1e-16 고유값은 5D track에서 6D Cartesian으로의 표현에서 생기는 rank 문제와 구분했고, 표에는 유의한 음수 최솟값을 적었다.

|event|candidate|slow key|min eig 5x5|max abs corr 5x5|min eig fit 직전 7x7|재fit valid|
|---|---:|---:|---:|---:|---:|---|
|320503141|2|348|-1.028606951|2.028596638|-0.675944005|D0 및 D*: true|
|320506888|0|35|-0.053974916|1.046534393|-0.004867442|D0 및 D*: true|
|320506804|0|484|-0.470731951|1.470642595|-0.009699194|D0 및 D*: true|

각 단계 값, 행렬 원소, eigenvalues, fit 결과는 `probe.jsonl`, `comparison.json`에 저장했다. 7개 후보 모두 saved vs input 5x5 차이=0, 재현 fitted state vs 저장 state 차이=0.

대표 event 320503141/candidate2:

|단계|slow pT GeV|eta|phi rad|
|---|---:|---:|---:|
|original|0.581054687500|0.297555476427|1.371715784073|
|pre_fit|0.581054679507|0.297555479263|1.371715785382|
|refit|0.580552031748|0.300548984331|1.375398838714|

이 후보에서 ΔmOriginal=0.145919097673 GeV, ΔmRefit=0.146094654308 GeV, δmD0=+0.000003382726 GeV, δmD*=+0.000178939361 GeV이다. δ(Δm)=+0.000175556635 GeV. 이는 중앙 bin 사례로 지정한 것이 아니라 covariance 입력/출력 재현 사례다.

#### 4. invalid/실패/재사용 처리

D0 tree invalid → continue [DStarFitter.cc:608](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:608). D* tree/state/vertex/children invalid → continue [DStarFitter.cc:640](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:640), [DStarFitter.cc:649](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:649), [DStarFitter.cc:656](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:656), [DStarFitter.cc:674](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:674). 따라서 명시적으로 실패한 tree의 child를 다음 후보에서 쓰는 경로는 확인되지 않았다. 후보별 vector/tree/CC는 루프 안에서 새로 생성되며, producer는 event 끝에 resetAll()을 호출한다. analyzer 진단 array도 매 event 재초기화 [PATCompositeTreeProducer6.cc:1283](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc:1283). 7개 후보의 독립 replay가 정확히 일치하므로 이 표본에 이전 후보 state가 재사용되었다는 증거는 없다.

하지만 validity flag는 SPD 검사와 다르다. KinematicState와 KinematicParametersError 생성자는 해당 flag를 true로 설정하며 eigenvalue 검사를 하지 않는다. Linearized-state weight는 일반 `cov.Inverse(error)`로 계산 [ExtendedPerigeeTrajectoryError.h:28](/cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src/RecoVertex/KinematicFitPrimitives/interface/ExtendedPerigeeTrajectoryError.h:28). KalmanVertexUpdator는 역행렬 실패를 invalid로 반환하지만 [KalmanVertexUpdator.cc:76](/cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src/RecoVertex/KalmanVertexFit/src/KalmanVertexUpdator.cc:76), invertPosDefMatrix는 Cholesky 실패 시 일반 Invert로 fallback한다 [invertPosDefMatrix.h:9](/cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src/DataFormats/Math/interface/invertPosDefMatrix.h:9). 이는 covariance 보정이 아니며 indefinite 행렬이 반드시 거부되는 것도 아니다. 이번 세 matched 후보에서 실제로 indefinite input + valid final fit이 함께 재현됐다.

Producer에 포괄적 `std::isfinite`/SPD gate는 없다. NaN에 대한 `<`, `>` 비교는 false이므로 일부 cut이 NaN을 잡지 못할 가능성이 있다. 마지막 mass window의 양쪽 비교는 finite하지 않은 mass를 통과시키지 않지만, 그것이 모든 covariance/chi2 원소의 유효성을 검증하는 것은 아니다. 이 부분은 코드상 가능성으로, 이번 표본에 NaN-fit 사용이 있었다는 뜻은 아니다.

**original과 fitted 상태 혼합은 실제 존재한다.** 다음 정의의 legacy output 구성이다. 실패 fallback 때문이라고 해석하면 안 된다. `rejectDuplicateSlowPion=False`도 실제 설정이므로 daughter TrackRef 재사용 조합이 설정상 배제되는 것은 아니다; 이번 covariance 문제와 인과관계는 확인하지 않았다.

#### 5. nominal / 진단 state / Δm / cos 정의

|값|실제 정의|
|---|---|
|nominal `mass`, `pT`, `eta`, `phi`, `y`|DStar candidate p4. momentum은 D* fitted parent의 GlobalVector; energy는 fitted D0 momentum+원래 저장 D0 mass로 계산한 energy와 fitted slow momentum+고정 pion mass로 계산한 energy를 float로 합한 것. fitted parent state의 mass를 그대로 저장한 것이 아님.|
|nominal `massDaugther1`, `pTD1`, `EtaD1`, `PhiD1`|원래 `theD0` candidate 그대로. 이는 최초 D0 vertex fit 후 저장된 D0이며 모든 track이 raw인 상태가 아님. D* fit에서 갱신된 D0 child가 아님.|
|nominal `pTD2`, `EtaD2`, `PhiD2`|D* fit 후 slow child momentum. TrackRef와 ptError/hit 정보는 원본 track에서 읽음.|
|`diagDStarFit`|D* fit top의 KinematicParameters 직접 state. mass는 fitted state mass.|
|`diagD0Refit`|D* fit child0의 KinematicParameters 직접 state. 원래 D0 candidate와 별도.|
|`diagSlowRefit`|D* fit child1 직접 state. nominal slow와 float GlobalVector rounding 및 mass-energy 구성 차이가 있음.|
|`integrityDmLegacy`|nominal candidate mass − original stored D0 mass, double에서 계산. 기존 float `mass-massDaugther1`의 값과 마지막 자리 rounding 차이 가능.|
|`deltaMOriginal`|M(original stored D0 p4 + original slow TrackRef p4) − M(original stored D0).|
|`deltaMRefit`|M(diagD0Refit p4 + diagSlowRefit p4) − M(diagD0Refit).|

근거: nominal p4 구성 [DStarFitter.cc:701](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:701)–713, daughter 저장 [DStarFitter.cc:805](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:805) 및 [DStarFitter.cc:842](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:842), analyzer [PATCompositeTreeProducer6.cc:1307](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc:1307), [PATCompositeTreeProducer6.cc:1516](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc:1516), [PATCompositeTreeProducer6.cc:1645](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc:1645), diagnostic 계산 [PAT6RefitDiagnostics.icc:82](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PAT6RefitDiagnostics.icc:82)–94. nominal D* p4는 저장한 nominal D0 daughter와 slow daughter p4의 합과 일반적으로 일치하지 않을 수 있다. 이는 코드에 명시된 mixed-stage 정의다.

이 production PAT6에는 **cosθ*EP 계산 또는 해당 branch가 없다**. `3DCosPointingAngle/2DCosPointingAngle`은 flight pointing이며 cosθ*EP와 다르다. EP Q/angle와 p4 구성요소를 저장하고 후속 분석에서 cosθ*EP를 계산한다. 그러므로 사용자의 19.80/19.39/15.56%에서 rest-frame boost에 nominal parent를 썼는지 daughter sum을 썼는지, D0를 무엇으로 고정했는지, EP side/calibration 및 cos bin을 고정했는지는 downstream 스크립트 없이는 확정할 수 없다. production nominal branch 기반 계산이라면 parent=legacy mixed p4, D0=first-fit stored p4, slow=final fitted momentum의 조합이라는 점을 반드시 대조해야 한다.

#### 재현 산출물과 추가로 필요한 식별자

- `probe_cfg.py`: 원래 production PR4 PSet을 로드하고 input file/events와 출력 경로를 지정, 별도 read-only probe를 path 끝에 추가.
- `RefitInputProbe.cc`, `BuildFile.xml`: 별도 점검 모듈 사본. unpacked output에서 track→packed pointer association까지 추적하고, 같은 field/mass hypothesis/default fitter로 재fit.
- `probe.jsonl`: fit 직전 raw 5x5/Cartesian 6x6/kinematic 7x7와 원래 저장·재fit state.
- `probe.root`: 세 이벤트의 원래 분석 output. `comparison.json`, `comparison_summary.txt`: 기존 validation ROOT와 수치 대조.
- `cmsRun.log`, `build.log`, `provenance_check.json`.

문제 4,364후보를 직접 연결하려면 결과 ROOT URL/목록 또는 selection table, 원본 `diagInputFile` LFN, run/lumi/diagEvent64, diagCandidateIndex, integrityD0Index, 세 track의 ProductID/key가 필요하다. 중앙 Δm bin의 정확한 edge, Δm branch/계산식, cosθ*EP 계산 스크립트와 EP calibration/version, 추가 selection/weight도 필요하다. 특히 η·φ만 복원할 때 cos bin을 nominal로 고정했는지 재계산했는지 구분해야 한다.

기존 branch에는 factory 직전 7x7와 packed association/schema가 없으므로 기존 ROOT만으로 모든 4,364개의 그 단계 검사는 할 수 없다. 이번처럼 해당 원본 이벤트를 읽는 probe면 된다. 향후 추가 저장이 필요하다면 `diagSlowPreFitState`, `diagSlowPreFitCov`(7x7), `diagSlowInitialCurvilinearCov`(5x5), `diagSlowImpactCartesianCov`(6x6), `diagSlowPackedProductID/Key`, `diagSlowHasTrackDetails`, `diagSlowCovarianceVersion/Schema`, factory/propagation validity가 정확한 항목이다. 지금 전체 production branch나 설정에는 이를 추가하지 않았다.
<!-- source:SRC-04:end -->

<a id="src-05"></a>

## SRC-05 — bdt_refit_causality_20260929/COVARIANCE_AUDIT_PLAN.md

**기록 구분:** 계획·제안 원문 (완료의 증거 아님). **원문:** [bdt_refit_causality_20260929/COVARIANCE_AUDIT_PLAN.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/COVARIANCE_AUDIT_PLAN.md). **SHA256:** `bad5f29c1ce17ef220631c8eb2ff45edf36bac9046e101b4d670d944dc2d5198`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-05:start -->
### Full pilot covariance audit

Done: audit all 20 completed jobs (2000 stored events), using all available geometrically GEN-matched D* candidates without target-bin cuts. Count original-to-packed loss of positive semidefiniteness, separately for slow pion and D0 daughters; check exact identity and unchanged-fit closure; quantify paired delta-mass changes when restoring original covariance. Save counts, examples, candidate-level CSV and a report. Do not interpret this as target-bin FWHM validation or uncertainty coverage.

Steps/files: read `parallel_pilot_complete_records.json`; implement and run `audit_pilot_covariance.py` with numpy; write `pilot_covariance_summary.json`, `pilot_covariance_candidates.csv`, and `PILOT_COVARIANCE_REPORT.md`; append the findings to REPORT/STATUS and mirror them to the existing EOS study.

Checks: all 20 job validations and 2000 event identities; all candidate original-track identities; finite symmetric covariance matrices with positive diagonals; eigenvalues of unit-diagonal correlation matrices with tolerance -1e-10; separate duplicate track instances from unique event/track keys; report invalid refits and closure residuals. Preserve the baseline and make no production modifications or further submissions.
<!-- source:SRC-05:end -->

<a id="src-06"></a>

## SRC-06 — bdt_refit_causality_20260929/EXISTING_RECO_PLAN.md

**기록 구분:** 계획·제안 원문 (완료의 증거 아님). **원문:** [bdt_refit_causality_20260929/EXISTING_RECO_PLAN.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/EXISTING_RECO_PLAN.md). **SHA256:** `7ef740fba285df3c776ac04828cd7769cd4d3414720fb66b5122d96ed9b5159e`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-06:start -->
### Existing RECO first — 2026-09-29

Done: inspect the retained 100 PR + 100 NPR events, establish exact original/packed track identity, and measure usable D* candidates and paired refit differences. If statistics or inputs are insufficient, submit only a validated, bounded parallel Condor pilot and record job IDs, paths and queue state. No claim of a physics cause from a small sample.

Authorization: Junseok explicitly authorized checking existing data first, then parallel Condor submission if needed. This supersedes the previous pending pilot approval; new production is conditional on the check.

Constraints: study-only changes, preserve canonical production and benchmark files, no ROOT macro compilation, no approximate matching passed off as exact identity, no fallback treatments.

1. Read benchmark event content and production config sections; inspect exact PF/track associations and runtime availability.
2. Add study-only CMSSW diagnostic/config and run on existing events. Verify identity and unchanged-covariance closure before alternatives. Preserve failures and denominators.
3. Inspect output counts by scope and decide whether new simulation is necessary. First prefer PAT/reconstruction on retained inputs.
4. For needed extra work, validate a small job then submit through KU Condor from gpusystem. Check executable/runtime/data accessibility, dry run, queue and output destinations.
5. Append REPORT.md and STATUS.md, documenting evidence, limitations, and actual submission status.

Files: this plan; study plugin/config/scripts; study outputs/logs; REPORT.md, STATUS.md and LARGE_WORK_PROPOSAL.md. Commands: SSH read-only inventory; isolated scram build (plugins only); cmsRun; Python/uproot summaries; Condor dry-run/submit/q if necessary.
<!-- source:SRC-06:end -->

<a id="src-07"></a>

## SRC-07 — bdt_refit_causality_20260929/EXISTING_RECO_REPORT.md

**기록 구분:** 조사 보고서·해석 (표본·시점별 결과). **원문:** [bdt_refit_causality_20260929/EXISTING_RECO_REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/EXISTING_RECO_REPORT.md). **SHA256:** `48d8b38ab469c81f4713392cb1150c7b3b4107b6acc4a4ff07aa03588c76a286`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-07:start -->
### Preserved RECO: exact original/packed comparison (2026-09-29)

#### Confirmed results

Reprocessed the retained `Prompt_100_11/step3.root` and `Nonprompt_100_21/step3.root` (100 events each). No new generation or detector simulation was needed. PAT was run in the original CMSSW_13_2_16_patch1 release, adding output retention of original tracks, PF candidates, and EDM associations. D0/D* production and the diagnostic plugin ran in the isolated study CMSSW_13_2_11 runtime.

| Quantity | Prompt | Nonprompt |
|---|---:|---:|
| Input events audited | 100 | 100 |
| Reconstructed D* candidates, before GEN matching | 280 | 312 |
| Candidates geometrically matched to the correct K/pi/slow GEN decay assignment | 8 | 6 |
| Events containing those candidates | 7 | 6 |
| Candidates with exact original-track identities for all three tracks | 8 | 6 |
| Matched candidates at reco pT 7–10 GeV/c | 1 | 2 |
| Also with abs(y)<0.3 | 0 | 1 |
| Also with centrality 0–10%, before cosine/DCA cuts | **0** | **0** |

Therefore the original target (pT 7–10, abs(y)<0.3, centrality 0–10%, tracker-axis abs(cos theta*) 0.6–0.8, DCA 0–0.08 cm) has no candidate in this sample. These results cannot measure its FWHM or establish its causal origin.

Exact original/packed track identity comes from EDM product IDs and association keys, **not** delta-R. GEN matching is separately a geometric selection (same charge, delta-R<0.03 for all three daughters with the assigned K/pi mass hypothesis); it is not independent hit-based truth. Electron/GSF packed tracks without the appropriate original GSF input are explicitly left unmatched, not substituted by generalTracks.

Each selected nominal candidate is frozen for all comparisons, including its BDT score, kinematics, and track identities. The analyzer records all input events on a separate path, including those rejected by the nominal analysis path; its broad candidate counts should not be interpreted as the final analysis yield.

#### Controlled refits and closure

Five modes were evaluated with fresh D0 and D* fits: packed baseline, reconstruction of the identical reco::Track, original slow-pion covariance only, original covariance for all three tracks, and all three complete original RECO tracks. No PSD regularization or adjusted boundary was applied.

All **70/70** refits (14 candidates times five modes) produced valid D0/D* trees. The identical-track control changed the stored slow-pion state by exactly zero. The independent packed replay differed from the production stored slow-pion state by at most 9.55e-17 over the stored components. All four cmsRun FrameworkJobReports (two PAT, two analysis) have no FrameworkError and all 200 input events appear in the audit output.

Shift below is `(M(D*)-M(D0))_alternative - (M(D*)-M(D0))_packed`, using the fitted D0 child in the final D* tree, in MeV.

| Intervention | PR shift range | NPR shift range |
|---|---:|---:|
| Identical-track control | 0 | 0 |
| Original slow-pion covariance only | −0.04217 to +0.13332 | −0.25493 to +0.03838 |
| Original covariance of all three tracks | −0.03611 to +0.13484 | −0.25500 to +0.03744 |
| All three original tracks, including their state | −0.03778 to +0.13506 | −0.25889 to +0.04482 |

The original covariance is expressed in the same five track-parameter coordinates. Covariance-only replacement holds the packed reference state fixed; complete-original-track replacement additionally changes the compressed reference state/momentum. These are distinct interventions and are both retained.

These 14 are broad diagnostic candidates before final mass/cosine/DCA selections. The largest slow-covariance shift is the NPR candidate `(run,lumi,event,index)=(21,1,937,0)`, pT=5.70373 GeV/c, y=0.29359, centrality bin 49 (24.5–25%), BDT=0.98082: fitted delta mass 145.89841 → 145.64348 MeV. It is outside the target pT/centrality selection. The PR maximum quoted in the table has packed fitted delta mass 157.81716 MeV, outside the earlier 140–153 MeV study window. Per-candidate coordinates and shifts are preserved in `existing_reco/paired_shifts.csv`.

The original covariance matrices of all 42 selected track instances are positive definite. Among the 14 packed slow-pion instances, **8 are indefinite** (PR4, NPR4); another three packed D0-daughter instances are indefinite. Counts are candidate-track instances, not unique tracks. This extends the earlier generic-track packing experiment to actual selected D* candidates.

**Confirmed:** original-covariance replacement changes the fitted delta mass for this small, broad sample, with a maximum absolute slow-covariance-only shift of 0.25493 MeV. The earlier 0.00216 MeV bound from `pseudoPosDefTrack()` on a different 24-candidate sample was a regularization sensitivity test; it was not a bound on restoring the true original covariance.

**Not established:** that covariance packing explains the prompt/nonprompt FWHM difference in the target bin, its population-average effect, the responsible BDT input, or uncertainty coverage. Positive definiteness alone does not validate a covariance or a resulting uncertainty. No final physics production treatment follows from these 14 candidates.

#### Official implementation evidence

The [CMSSW PATPackedCandidateProducer, lines 397–416](https://github.com/cms-sw/cmssw/blob/CMSSW_13_2_16_patch1/PhysicsTools/PatAlgos/plugins/PATPackedCandidateProducer.cc#L397-L416) explicitly provides the track association: “include also the mapping track -> packed PFCand”. The retained association is inverted by exact product/key identity in the study.

The [CMSSW PATLostTracks, lines 220–244](https://github.com/cms-sw/cmssw/blob/CMSSW_13_2_16_patch1/PhysicsTools/PatAlgos/plugins/PATLostTracks.cc#L220-L244) constructs the corresponding lost-track association and notes “not done for the lostTrack:eleTracks collection”. Unsupported identities are reported rather than guessed.

#### Additional work decision

The existing sample validates the comparison method but has zero target-bin candidates. Additional production is needed to study the target distribution. A bounded broad-acceptance pilot was submitted to **CERN HTCondor cluster 12717346**: PR1000 + NPR1000 accepted stored events planned, split into 20 independent 100-event jobs with distinct run IDs and random seeds. The same generator, decay, pT/rapidity-filter and HYDJET settings as the benchmark are retained. Each job keeps RECO, associated MiniAOD, analysis ROOT, paired records, logs, configs and event-identity checks.

This pilot should increase broad-sample diagnostics; it **does not guarantee adequate target-bin FWHM precision**. No quantitative target yield is inferred from zero observations. The reused 325-event HYDJET input also limits background independence. No automatic production beyond this pilot is authorized by this study plan.

User authorized conditional parallel submission after the existing-data check. CERN HTCondor is used for this existing lxplus production task: the KU submit host and checked worker lack CMSSW CVMFS, while the CERN inputs/runtime are available. The additional site-permission question was redundant with this authorization and was not treated as a new approval gate. First queue check at 2026-09-28 23:05:12 UTC: **20 idle, zero held**. Submission is confirmed; generated physics results are not yet available. `parallel_pilot_submission.json` records submission/log/output paths and immutable worker/bundle hashes.

The 20-job dry run passed. Configurations for all four simulation/PAT stages parse in their correct releases. The packaged, relocated CMSSW runtime reprocessed existing PR100 and reproduced its 108 JSONL records byte for byte (100 events + 8 candidates). The event-identity validator accepted the preserved baseline fixtures and rejected a deliberately mismatched event ID at unchanged event count; fixtures are not mislabeled as newly generated pilot output. CERN native delegated Kerberos credentials were confirmed valid and an actual checksum-verified EOS upload succeeded. A separate GSI-only EOS test failed; the pilot relies on CERN's native credential delegation for EOS, not that failed test.

#### Reproducibility and failures

Local scripts: `OriginalCovarianceProbe.cc`, `prepare_existing_reco_remote.py`, `run_existing_analysis_remote.py`. Local evidence: `existing_reco/summary.json`, PR/NPR `pairs.jsonl`, and four FrameworkJobReports. Remote configs, logs and ROOT outputs are under `bdt_refit_causality_20260929/existing_reco/` on EOS.

The first PAT launch referenced a benchmark runtime that had been removed. No ROOT was produced despite its wrapper returning zero; this is explicitly not a successful run. A study-local runtime was restored, and output/FJR checks were added. The first plugin build rejected a missing std::array initializer brace; the corrected build passed. Canonical production and retained benchmark inputs were not modified.

최종 제출 후 확인: cluster `12717346`의 **20/20 작업 Running, Held 0**. 실행 중이며 산출물/physics QA 완료를 뜻하지 않는다.
<!-- source:SRC-07:end -->

<a id="src-08"></a>

## SRC-08 — bdt_refit_causality_20260929/FOLLOWUP_PLAN.md

**기록 구분:** 계획·제안 원문 (완료의 증거 아님). **원문:** [bdt_refit_causality_20260929/FOLLOWUP_PLAN.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/FOLLOWUP_PLAN.md). **SHA256:** `01a23545db2c2536f324ddf29dd954cd2e38a0a120c9c049da3e2b04a9a5737a`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-08:start -->
### Follow-up diagnosis, 2026-09-29

Done for this stage means: perform all bounded studies available from the existing candidate/diagnostic data, explicitly test competing mechanisms, update the report with accepted/rejected/unresolved hypotheses, and specify only the substantial additional production needed for a stronger conclusion. No claim of a unique cause without evidence. Canonical production remains unchanged.

1. Extend exact diagnostic joins to all five cosine bins for official PR/NPR pT4 (6,159 baseline candidates, 1,621 files). Run `python3 join_diagnostics.py --all-cos`; preserve prior core-bin results.
2. Add `diagnose_followup.py`: paired stage and angular-component decomposition; covariance/GEN/matching/duplicate checks; kinematic and topology standardization with common-support diagnostics; cut, window and weight sensitivity; event-cluster uncertainty. No new mass fit or corrective production treatment.
3. Compare official/private and source families using already available loose-candidate data. Keep diagnostic-source coverage explicit.
4. Create plots and numerical artifacts, check arithmetic/identity/invariance, inspect plots. Use current full-data outputs rather than rerun old stages unnecessarily.
5. Append results, a hypothesis-and-validation table and progress checklist to REPORT.md locally and on lxplus. Keep failed checks and support limitations visible.

Stop conditions: bounded extraction and analyses complete; methods without adequate common support are reported as uninformative, not repaired by arbitrary weights. Full RECO/truth regeneration, large campaigns and statistical-power production requests require Junseok's approval before submission. No background daemons or canonical production edits.
<!-- source:SRC-08:end -->

<a id="src-09"></a>

## SRC-09 — bdt_refit_causality_20260929/FOLLOWUP_REPORT.md

**기록 구분:** 조사 보고서·해석 (표본·시점별 결과). **원문:** [bdt_refit_causality_20260929/FOLLOWUP_REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/FOLLOWUP_REPORT.md). **SHA256:** `34dcee7b31dcf464500f869dcaa9434c83757bc6c58863c74154305b69737a5e`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-09:start -->
> 후속 결과: 보존 RECO 200이벤트의 exact covariance 비교 완료, CERN Condor cluster 12717346에 20-job pilot 제출. 최신 상태는 `EXISTING_RECO_REPORT.md`와 `STATUS.md` 참조. 아래는 앞선 진단 단계의 기록입니다.

### 원인 진단 후속 리포트 — 2026-09-29

#### 최종 판단

이번 단계에서 관측된 피크 중심 집중의 직접적인 운동학적 경로는 **BDT가 선택하는 D0 후보 집단과 D* vertex refit의 slow-pion 방향 변화의 결합**으로 좁혀졌다. 다만 이것을 특정 BDT 입력 하나 또는 특정 수치적 결함 하나의 인과효과로 확정할 수는 없다. 공식 pT4 표본의 cos 0.6–0.8에서 pass−fail 차이는 4.98 ± 3.34 percentage points(pp)이며, 다섯 cos × 두 family를 함께 고려한 95% 동시 구간도 0을 포함한다. 통계적 변동 가능성은 남아 있다.

별도로 **정상 RECO covariance가 MiniAOD 방식의 압축·복원으로 indefinite가 되는 경로는 통제 실험에서 직접 재현했다.** 그러나 실제 문제 표본의 24개 후보를 재생산하여 covariance를 바꿔 본 결과, CMSSW의 `pseudoPosDefTrack()` 처리는 중앙-bin 소속을 하나도 바꾸지 않았다. 그러므로 covariance가 잘못된 확률오차 행렬이라는 사실과, 이것이 이번 BDT-dependent peak의 주원인이라는 주장은 서로 다르다.

현재 결론은 원인의 전부를 해결했다는 뜻이 아니다. 작은 자료 재분석·재생산으로 할 수 있는 분리는 완료했고, 남은 결정적 검증에는 **동일한 D* daughter track의 packing 전 covariance와 독립 TrackingParticle truth를 함께 보존한 표본**이 필요하다. 검증되지 않은 covariance 처리를 nominal production에 적용하지 않았다.

#### 진행 체크리스트

- [x] 기존 production/vertex-refit 및 181-candidate causal follow-up 리포트 확인.
- [x] official PR/NPR pT4의 모든 다섯 cos 구간 6,159/6,159 후보 연결. 6,155개는 기존 진단 출력, 4개는 원본 MiniAOD 재생산으로 복구. 원래 core-bin 1,281개 값은 모두 정확히 동일.
- [x] source LFN·run/lumi/event·후보 운동학 및 Δm 일치 확인; 누락 0, 연결 오류 0.
- [x] BDT 0.95 producer cut 직접 개입: 24개 중 12개 제거, 남은 12개 31개 물리/진단 값 동일.
- [x] 5,747 event cluster, 5,000-replica bootstrap으로 모든 cos의 paired refit gain 및 동시 구간 계산.
- [x] D0 state / slow pT / η / φ 16개 조합, 24개 변경 순서 평균으로 성분 기여 계산; 합이 실제 변화와 수치적으로 일치.
- [x] 운동학·D0 topology·covariance 조건 맞춤, common support 및 잔여 표준화 평균차(SMD) 점검.
- [x] 가중치, source 혼합, GEN/slow-track 중복, 원래 GEN matching 경계, score 구간, peak window 폭·중심 이동 점검.
- [x] 전체 official+private 15,706개 loose 후보에서 source 분리 및 KDE FWHM 민감도 점검.
- [x] 실제 MiniAOD 24개 대상 × identity/4개 covariance 변경 = 120개 독립 D* refit 비교. 8/8 batch exit 0, 모두 valid; identity slow 4-vector 차이 0.
- [x] 별도 보존 RECO 20 events의 12,060 low-pT track에서 압축·복원 통제 실험. 원본/sparse-only/schema 8/schema 520 비교 완료.
- [x] 표·그림·실행 코드·로그·소스 hash·제한점과 승인 필요 작업을 기록.
- [ ] 문제 D* daughter 자체의 packing 전 원래 covariance를 이용한 paired refit: 해당 official 이벤트의 full RECO가 없어 미실시.
- [ ] 독립 track truth에 의한 잘못된 slow-pion 연결 배제 및 resolution pull 검증: 현재 문제 MiniAOD의 association은 비어 있어 미실시.
- [ ] 새 full-chain production/대량 MC 생성: 미제출, 별도 승인 필요.

위 완료 기준은 이 단계의 유한한 진단 범위다. 모든 가능한 물리 검증이나 최종 uncertainty validation을 완료했다는 뜻은 아니다.

#### 표본 및 정의

reco D* pT 7–10 GeV/c, |y|<0.3, centrality 0–10%, DCA 0–0.08 cm, nominal tracker |cosθ*|의 다섯 구간. truth-matched/non-swap, 기존 Δm 140–153 MeV 및 D0 mass 1811.20–1917.38 MeV 선택을 유지했다. paired state 비교는 official PR/NPR pT4만 사용한다. source/FWHM 비교만 전체 official+private source를 사용한다.

중앙 bin은 145.28813559322035 ≤ Δm < 145.5084745762712 MeV로 고정했다. `gain`은 같은 후보 집단에서 refit 후 중앙 비율 − refit 전 중앙 비율이다. `contrast`는 BDT≥0.95 gain − 0≤BDT<0.95 gain이다. 이는 fitted FWHM, 유의도, detector resolution과 같은 양이 아니다.

후보와 nominal cosine/weight는 reconstruction stage 사이에서 고정했다. weight는 현재 campaign 값을 한 번 사용했다. event cluster는 source/run/lumi/event로 정의했다. 원래 1,000-replica 보고와 후속 5,000-replica 결과의 오차 소수점 차이는 bootstrap 반복 수 및 전체 event 집합이 바뀐 데 따른 Monte Carlo 차이이며, 중심값 정의는 바뀌지 않았다.

#### 모든 cos에서의 비교


| Family | |cosθ*| | Pass−fail gain (pp) | 동시 95% 구간 반폭 (pp) |
|---|---|---|---|
| PR | 0.0–0.2 | -1.75 ± 2.95 | 8.36 |
| PR | 0.2–0.4 | -3.22 ± 3.02 | 8.55 |
| PR | 0.4–0.6 | 2.28 ± 3.48 | 9.87 |
| PR | 0.6–0.8 | 4.98 ± 3.34 | 9.46 |
| PR | 0.8–1.0 | -2.63 ± 3.46 | 9.81 |
| NPR | 0.0–0.2 | -4.51 ± 3.62 | 10.26 |
| NPR | 0.2–0.4 | -1.63 ± 4.07 | 11.54 |
| NPR | 0.4–0.6 | -4.27 ± 3.87 | 10.98 |
| NPR | 0.6–0.8 | -0.12 ± 3.84 | 10.88 |
| NPR | 0.8–1.0 | -0.53 ± 3.97 | 11.26 |


10개 contrast의 bootstrap maximum absolute standardized deviation으로 동시 반폭을 정했다. 어느 구간도 이 비교에서 0을 배제하지 않는다. 여러 window/score 조건은 탐색적 진단이며 독립적인 discovery test로 사용하지 않는다. cos 0.6–0.8의 점추정이 다른 구간보다 커 보이는 것은 확인되지만, 특수한 물리적 이상이 통계적으로 입증된 것은 아니다.

#### 어떤 상태 변화가 피크를 움직이는가

Prompt cos 0.6–0.8, BDT≥0.95의 실제 gain +4.532 pp를 네 성분에 분배했다. 각 성분을 바꾸는 모든 순서를 평균한 Shapley allocation으로 순서 의존성을 제거했다.


| 성분 | 중앙 gain 기여 (pp) |
|---|---|
| D0_state | 0.017 ± 0.109 |
| slow_pT | 0.258 ± 0.193 |
| slow_eta | 1.033 ± 1.065 |
| slow_phi | 3.225 ± 1.587 |


η+φ 기여의 점추정 합은 4.257 pp로 전체 gain의 약 94%다. 이 수치는 해당 표본·중앙구간에 대한 운동학적 분해이며, 통계적으로 확정한 인과 기여율이 아니다. 성분 오차는 상관되어 있으므로 표의 오차를 독립적으로 합하지 않는다. D0 state와 slow pT 기여는 이 진단에서 작다. 단, 일부 후보의 D0 mass 변화까지 모두 작다는 주장은 하지 않는다.

수학적으로 `M(D0+π)^2 = mD0² + mπ² + 2(ED0 Eπ − pD0·pπ)`이므로, daughter D0 invariant mass가 비슷해도 상대 운동량 방향이 바뀌면 D* Δm은 바뀐다. CMS [Vertex Fitting 안내, 도입부](https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideVertexFitting)의 “constrained track parameters and their covariances”가 이 일반적 설명의 근거다. 이 공식 문서가 이번 표본의 특수한 BDT 의존성까지 검증해 주는 것은 아니다.

#### 운동학과 topology를 맞춘 비교

결과를 보지 않고 각 변수의 weighted quantile cell을 구성하고, pass와 fail 각각 원래 후보가 최소 5개인 cell만 사용했다. 두 집단의 cell 확률 중 작은 값을 공통 목표분포로 삼았다. bootstrap마다 cell 가중치를 재계산하고, cell 경계와 support 선택은 고정했다. 아래는 prompt cos 0.6–0.8이다.


| 조건 | 통과 후보 보존 | 같은 support, 조정 전 (pp) | 조정 후 (pp) | 조정 변수 최대 |SMD| |
|---|---|---|---|---|
| Dstar_kinematics | 74.4% | 4.21 | 3.11 ± 3.55 | 0.071 |
| slow_kinematics | 88.7% | 5.36 | 5.27 ± 3.57 | 0.082 |
| D0_topology | 18.3% | 6.47 | 5.54 ± 6.44 | 0.577 |
| D0_pointing_only | 100.0% | 4.98 | 4.87 ± 4.24 | 0.367 |
| D0_decay_only | 100.0% | 4.98 | 4.76 ± 4.33 | 0.173 |
| covariance | 90.3% | 3.40 | 2.21 ± 3.28 | 0.068 |
| joint_coarse | 36.5% | 4.88 | 5.02 ± 5.36 | 0.788 |


D* 운동학 조정은 contrast를 4.21→3.11 pp로 낮추지만 오차가 크다. slow-pion 운동학만 맞추어도 차이는 사라지지 않는다. 이것은 해당 변수들이 무관하다는 증명이 아니다.

D0 topology 세 변수를 동시에 맞추면 pass 후보의 18.3%만 남고, 잔여 최대 |SMD|도 0.577로 크다. 즉 **원인 분리를 위한 비교 조건이 충분히 맞지 않았다.** 이런 결과를 보고 특정 topology 입력이 원인이라고 하거나, 반대로 topology 원인이 배제됐다고 결론내릴 수 없다. joint coarse 조건도 같은 제한이 있다. pointing/decay 단독 coarse matching 역시 잔여 불균형이 남는다.

#### 간단한 대안 설명 점검


| 검사 | Prompt core contrast (pp) |
|---|---|
| baseline | 4.98 ± 3.34 |
| unit_weights | 4.93 ± 3.32 |
| GEN_multiplicity_weight | 5.53 ± 3.44 |
| slow_multiplicity_weight | 5.10 ± 3.35 |
| original_GEN_dR_lt_0p03 | 5.03 ± 3.36 |
| both_GEN_dR_lt_0p01 | 7.26 ± 3.97 |
| PSD_only | 8.30 ± 4.99 |
| indefinite_only | 3.23 ± 4.33 |


GEN/slow 중복 multiplicity는 모든 다섯 cos를 합친 loose 표본에서 정의했다. 따라서 core만 사용한 이전 중복 보정과 값이 약간 다르다. 이들은 진단 가중치이며 production weight를 변경하지 않았다. 원래 slow track이 같은 accepted GEN에 대해 ΔR<0.03을 통과하지 못한 core 후보는 pass 5개/fail 1개이며, 그중 중앙 유입은 0개다. 따라서 이 단순 matching 경계 이동은 관측된 중앙 유입을 설명하지 않는다. fitted-based geometric matching의 전체 bias나 다른 track의 잘못된 연결까지 배제한 것은 아니다.

source 혼합 분해는 `tight−loose = within-source 변화 + source 비율 변화`의 정확한 대수적 항등식이다. 전체 official+private core prompt의 중앙 비율 변화는 +1.150 pp이며, source 비율 변화 항은 +0.027 pp, within-source 항은 +1.123 pp다. 따라서 단순 혼합 비율 변화가 이 중앙 비율 변화의 주원인이라는 설명은 지지되지 않는다. 이것은 source 내부 모델 차이를 전부 배제하는 검사는 아니다.

#### FWHM 숫자의 강건성

기존 DSCB FWHM 0.9095→0.6542 MeV와 달리 empirical width68은 1.4966→1.5212 MeV였다. 전체 분포가 일괄적으로 좁아진 상황과 맞지 않는다. 같은 nested 표본을 Gaussian KDE로 평활화하고 event-paired bootstrap 200회로 tight−loose FWHM 차이를 계산했다. KDE는 별도의 검출기 resolution model이 아니며 bandwidth를 0으로 외삽하지 않았다.


| Family | Bandwidth (MeV) | Loose → tight FWHM (MeV) | 차이 ± bootstrap SD (MeV) |
|---|---|---|---|
| PR | 0.1 | 0.925 → 0.816 | -0.109 ± 0.078 |
| PR | 0.15 | 1.055 → 0.978 | -0.077 ± 0.062 |
| PR | 0.2 | 1.171 → 1.121 | -0.050 ± 0.048 |
| PR | 0.3 | 1.382 → 1.358 | -0.024 ± 0.035 |
| NPR | 0.1 | 1.043 → 1.134 | 0.090 ± 0.071 |
| NPR | 0.15 | 1.114 → 1.177 | 0.064 ± 0.049 |
| NPR | 0.2 | 1.191 → 1.237 | 0.046 ± 0.039 |
| NPR | 0.3 | 1.366 → 1.393 | 0.026 ± 0.028 |


Prompt가 좁아지는 방향은 유지되지만 정도는 평활화에 민감하고 모든 표시 bandwidth의 bootstrap percentile 구간은 0을 포함한다. **특정 DSCB FWHM 감소율을 detector resolution 변화율로 해석하면 안 된다.** 이 검사만으로 기존 fit이 잘못됐다고 단정하는 것도 부당하다. fit FWHM 자체의 완전한 uncertainty/model study는 이번 비모수 비교와 구별한다.

#### Covariance 이상: 발생 경로와 피크 영향 분리

전체 6,159개 slow original covariance 중 3,924개가 indefinite다. core 1,281개에서는 λ–dsz 상관계수 |ρ14|>1이 612개, φ–dxy |ρ23|>1이 631개다(서로 중복 가능). 최소 normalized eigenvalue는 약 -1.43이고 |ρ|는 최대 약 2.43이다. 이는 단순 부동소수점 오차로 0을 조금 넘은 경우가 아니다. covariance의 필수 조건 `Cii*Cjj − Cij² >= 0`를 위반한다.

이번 24개 실제 MiniAOD 후보의 packed covariance version은 모두 1, schema는 모두 520이었다. [CMSSW 13_2_11 PackedCandidate.cc, packCovariance/unpackCovariance](https://github.com/cms-sw/cmssw/blob/CMSSW_13_2_11/DataFormats/PatCandidates/src/PackedCandidate.cc#L75-L115)는 다섯 diagonal과 세 off-diagonal만 저장·복원한다. [CovarianceParameterization.cc, pack/unpack](https://github.com/cms-sw/cmssw/blob/CMSSW_13_2_11/DataFormats/PatCandidates/src/CovarianceParameterization.cc#L168-L199)는 원소별 reference 비율을 압축한다.

실제 release 데이터파일의 schema 520에서 세 운동량 diagonal의 log quantizer `base`는 2, λ–dsz 및 φ–dxy는 8이었다. ROOT 설정 이름은 `bit`지만 [liblogintpack.h, pack16log/unpack16log](https://github.com/cms-sw/cmssw/blob/CMSSW_13_2_11/DataFormats/Math/interface/liblogintpack.h#L23-L65)의 인자는 `base`다. 이를 '2-bit 정밀도'라고 읽으면 안 된다. 원소를 각각 근사하므로 행렬 전체의 positive semidefiniteness가 자동으로 보장되지 않는다.

##### 실제 RECO track을 이용한 통제 round-trip

별도 private full-chain benchmark의 PR/NPR 각각 첫 10 events를 사용했다. highPurity, pixel hit 존재, 0.5≤pT<0.95 GeV, |η|<2.4인 실제 generalTracks를 pion 가정으로 PackedCandidate에 넣고 같은 CMSSW library로 압축·복원했다. 주로 underlying-event track을 포함하는 **일반 track 표본**이며 문제 D* daughter 표본이 아니다.

비교: 원래 full covariance / 저장하지 않는 cross term만 0으로 만든 sparse-only / 고정 schema 8 round-trip / 고정 schema 520 round-trip. schema 8은 해당 release 설정의 high-quality 경로이며 schema 520보다 세밀하지만, 이 사용 자체가 물리적으로 검증된 처방이라는 뜻은 아니다.


| Benchmark | Track N | 원본 indefinite | Sparse only | Schema 8 | Schema 520 |
|---|---|---|---|---|---|
| PR | 5644 | 0 | 703 | 3483 | 3436 |
| NPR | 6416 | 0 | 793 | 3894 | 3793 |


원래 12,060개가 모두 PSD였지만 schema 520 후 7,229개가 indefinite가 됐다. 원래 정보 손실과 원소별 압축이 실제 정상 covariance를 비정상으로 만들 수 있음을 직접 보인 것이다. Cross term 삭제만으로도 1,496개가 indefinite다. 두 효과를 단순 가산하여 원인을 몇 %씩 배정하지 않는다.

schema 8도 indefinite 개수 자체를 해소하지 못했다. 다만 normalized 최소 고유값 < -0.1인 심한 경우는 schema 520의 5,522개에서 schema 8의 290개로 줄었다. 즉 단순히 '정밀도를 높이면 모든 문제가 해결된다'는 주장도 성립하지 않는다.

이전 리포트의 full-RECO 부재는 **문제 official 후보와 동일한 이벤트**에 대한 판단이다. 이번 별도 benchmark RECO 발견으로 통제 round-trip은 가능해졌지만, 그 official 후보의 원래 covariance를 회수한 것은 아니다.

##### 같은 D* 후보의 covariance 개입

새 `CovarianceSensitivityProbe`에서 24개 frozen 대상마다 D0/D*를 처음부터 독립 refit했다. identity, `pseudoPosDefTrack()` covariance, diagonal-only, original covariance×0.5, ×2를 비교했다. 원래 track momentum/reference point와 질량 가정은 유지했다. 모두 120개 valid이고 identity slow 4-vector는 원래 replay와 정확히 일치했다. 이 sample은 deliberately stratified이므로 모집단 비율이나 평균효과 추정에 사용하지 않는다.


| 변경 | Valid N | 최대 |Δm 변화| (MeV) | 중앙 소속 변경 N |
|---|---|---|---|
| packed_posdef | 24 | 0.002156 | 0 |
| diagonal | 24 | 0.058706 | 1 |
| scale_half | 24 | 0.010664 | 0 |
| scale_double | 24 | 0.006178 | 0 |


`pseudoPosDefTrack()`는 해당 코드에서 음의 eigenvalue에 따라 diagonal을 올리는 regularization이다. [공식 구현](https://github.com/cms-sw/cmssw/blob/CMSSW_13_2_11/DataFormats/PatCandidates/src/PackedCandidate.cc#L155-L198)의 주석은 “alter values to allow for pos-def”이다. 원래 RECO covariance 복원이 아니다. 이 시험에서 작은 변화가 나온 것은 **그 특정 처리에 대한 민감도가 작은 것**이지, covariance가 물리적으로 정확하거나 어떤 가능한 covariance 오류에도 안전하다는 증거가 아니다. ×0.5/×2는 indefinite 행렬의 부호 문제를 없애지 않는 별도의 민감도 검사다.

#### 가설별 판정과 필요한 반증

| 가설 | 현재 판정 | 수행한 검증 | 남은 결정적 검증 |
|---|---|---|---|
| BDT cut이 남은 후보의 reconstruction 값을 직접 바꾼다 | 확인한 12 survivors에서 배제 | producer cut 전후 31개 값 exact equality | 큰 대표표본 일반화는 가능하나 현재 우선순위 낮음 |
| D0 topology 선택과 slow angular refit의 결합 | 직접 경로를 지지; 개별 입력 인과 미확정 | paired states, 모든 변경 순서, 조건 맞춤 | 충분한 common support와 pre-packing covariance/truth를 가진 같은 후보 비교 |
| D0 mass 폭 자체가 D* 차이의 주경로다 | 이번 중앙구간 분해에서는 작은 항 | D0 state 기여 +0.017 pp 대 angular +4.257 pp | 다른 bin/source에 대한 일반화는 별도 |
| source 혼합/weight가 주원인이다 | 단순 설명 지지되지 않음 | source-mixture 항 +0.027 pp, unit-weight 비교 | source별 generator/reco 모델 효과까지 배제하지 않음 |
| 중복 또는 원래 matching 경계 통과가 주원인이다 | 단순 설명 지지되지 않음 | multiplicity 보정, original matching 중앙 유입 0 | independent TrackingParticle association으로 잘못된 slow track 연결 확인 |
| MiniAOD packing이 covariance를 비정상으로 만든다 | 별도 실제-track 통제 실험에서 재현 | 0/12060 → 7229/12060 indefinite | 문제 후보 자체의 원본 covariance와 exact paired 비교 |
| 그 covariance 이상이 BDT-dependent peak의 주원인이다 | 입증되지 않음 | PSD도 중앙 유입; pos-def 처리 중앙 소속 0/24 변경 | 진짜 RECO covariance/track truth 사용 refit과 bias/pull 비교 |
| fitted FWHM 감소가 전체 resolution 개선이다 | 성립하지 않음 | empirical width68 증가, KDE bandwidth 민감도 | truth residual/coverage 및 shape-model uncertainty |
| 관측한 cos별 차이에 통계변동이 크게 기여한다 | 배제할 수 없음 | 10-cell 동시 구간 모두 0 포함 | 독립 MC와 사전 고정한 observable/검정 |

#### 다음 단계 및 자원 판단

추가 MC 수만 늘려도 이 sample의 통계는 개선된다. 그러나 동일한 packing을 반복한 MiniAOD만 늘리면 원래 covariance와 독립 track truth를 잃는 문제는 해결되지 않는다. 다음 생산은 두 정보를 함께 보존하도록 설계해야 한다.

현재 core contrast의 오차 약 3.34 pp를 1 pp까지 줄이려면 단순 독립 표본 scaling에서 약 11배의 유효 통계가 필요하다. 이를 raw GEN event 수와 동일시하지 않는다. topology common support 문제와 systematic/source 효과까지 해결되는 보장도 없다. 현재 점추정으로 유의도를 목표 삼아 stopping하는 설계는 권하지 않는다.

승인 요청은 `LARGE_WORK_PROPOSAL.md`에 구체화했다. 먼저 prompt/nonprompt 각각 1,000 filtered accepted events의 보존형 full-chain pilot으로 track identity/truth/packing 전후 비교 경로를 검증하고, 실제 대상-bin 수율과 자원을 측정한다. 이후 통계용 대량 production 규모는 그 결과로 다시 승인받는다. Pilot 자체로 문제-bin FWHM을 확정한다고 약속하지 않는다.

#### 코드·실행·재현성

모든 변경은 campaign study와 isolated lxplus study runtime에 한정했다. 기존 runtime 165개 파일은 이전 검증 상태와 hash가 동일하다. 후속 진단 plugin 두 개만 추가했다. 최종 소스 hash는 `followup_runtime_provenance_final.json`에 있다. canonical production 및 campaign nominal 결과는 수정하지 않았다.

새 source/control 진단 코드는 `diagnose_followup.py`, `diagnose_sources.py`; 전체 연결은 `join_diagnostics.py --all-cos`, 네 이벤트 복구 합치는 검증은 `all_cos/finalize_join.py`다. final input은 `all_cos/paired_candidates_complete.npz`이며, initial 4개 missing audit도 보존했다. `all_cos/complete_validation.json`이 최종 연결 기준이다.

주요 표: `followup_allcos/*.json,csv`, `source_diagnostics/*.json,csv`, `covariance_sensitivity_compact.json`, `packing_roundtrip_summary.json`, `packing_precision_detailed_summary.json`.
첫 baseline 재생산의 기존 1개 종료 anomaly와 setup 실패는 원래 리포트에 남아 있다. 이번 covariance 8개 batch와 packing의 PR/NPR 실행들은 모두 exit 0이다. 새 probe 첫 build의 TrackQuality 타입 오류는 생성자 호출을 수정한 뒤 build와 identity control로 검증했으며 production 결과로 사용하지 않았다.

Shapley 성분 합의 최대 오차는 중앙 gain 2.84e-14 pp, Δm 1.78e-11 MeV다. source-mixture 항등식, stage Δm, candidate join, replay identity를 assert로 검증했다. 완전한 최종 물리 uncertainty coverage 시험과는 구별한다. 그림은 PNG/PDF로 저장하고 잘림·겹침을 확인했다.



![Follow-up diagnosis](/home/jun502s/DstarAna/DStarAnalysis/Fit/PbPb/Test/SpinAlignment/output/campaign_26Sep28_official_private3M5M_source_family_1to1/study/bdt_refit_causality_20260929/followup_diagnosis.png)

![Packing round-trip](/home/jun502s/DstarAna/DStarAnalysis/Fit/PbPb/Test/SpinAlignment/output/campaign_26Sep28_official_private3M5M_source_family_1to1/study/bdt_refit_causality_20260929/packing_roundtrip.png)

Local: `/home/jun502s/DstarAna/DStarAnalysis/Fit/PbPb/Test/SpinAlignment/output/campaign_26Sep28_official_private3M5M_source_family_1to1/study/bdt_refit_causality_20260929`

Lxplus: `/eos/home-j/junseok/analysis/dstarana/VertexCompositeStudies/studies/refit_diagnostics_20260918/bdt_refit_causality_20260929`
<!-- source:SRC-09:end -->

<a id="src-10"></a>

## SRC-10 — bdt_refit_causality_20260929/LARGE_WORK_PROPOSAL.md

**기록 구분:** 계획·제안 원문 (완료의 증거 아님). **원문:** [bdt_refit_causality_20260929/LARGE_WORK_PROPOSAL.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/LARGE_WORK_PROPOSAL.md). **SHA256:** `a3a828721424b65d3e59ade9454bf8f8e2d8321c9209c8290878fa3f20c864dc`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-10:start -->
### 승인 요청 범위: packing 전후 covariance와 track truth를 보존하는 full-chain pilot

**이전 제안의 이력.** 이후 준석이 기존 자료를 먼저 확인하고 필요하면 Condor 병렬 제출하도록 승인했다. 보존 RECO 200이벤트에서 실제 D* 14개의 exact original/packed 비교를 완료했으나 목표 bin 후보는 0개였다. 현재 실행 계획/결과는 `EXISTING_RECO_REPORT.md`, `STATUS.md`와 submission receipt를 따른다. 기존의 승인 요청은 중복해서 적용하지 않는다.

현재 pilot은 원본 RECO track/covariance와 정확한 original/packed association을 보존한다. 독립 hit-based GEN truth association의 완성까지 검증됐다는 뜻은 아니며, 후보의 GEN 선택은 여전히 기하학적 매칭이다.

#### 왜 필요한가

현재 문제 official MiniAOD에는 같은 track의 원본 RECO covariance와 유효한 독립 track truth association이 없다. 별도 benchmark의 일반 track에서는 packing-induced indefinite covariance를 재현했지만, 이것으로 BDT-selected D* peak의 인과효과를 확정할 수 없다. 필요한 것은 같은 이벤트·같은 K/π/slow track에 대해 진짜 원본 covariance와 compressed covariance를 비교하는 출력이다.

#### 첫 승인 단위

- Prompt 1,000 + nonprompt 1,000 **generator filter를 통과한 stored events**, 합계 2,000. 재구성된 D* 2,000개가 아니다.
- 기존 private benchmark의 pThat>2 GeV, GEN D* pT>4 GeV, |y|<1.2 및 각 family의 원래 decay 설정 유지. 중앙ity/cos/BDT 결과를 보고 generator cut을 추가하지 않는다.
- 500 accepted events/job라면 4개 job에 해당한다. 실제 제출 전 runtime, configuration, source inputs, seeds/HYDJET provenance를 고정한다.
- RECO→PAT 구간에서 original TrackRef→packed/unpacked track identity map, 원본/압축 5×5 covariance, track parameters, TrackingParticle 및 모녀 관계, association quality를 보존한다.
- D0/D* reconstruction을 동일 후보에 대해 original/packed covariance로 비교한다. BDT inputs와 score, 모든 fit states/성공 여부/제거 사유를 기록한다. identity-matched denominator를 먼저 고정하고 실패를 제외한 모양만 비교하지 않는다.
- 후보 선택은 최소 BDT 0 및 0.95; 최종 문제-bin뿐 아니라 전체 접근 가능한 후보도 저장하여 pilot 검증력을 확보한다.

#### Pilot의 성공 기준

1. full-chain event identity와 mixing provenance 일치; baseline MiniAOD와 분석재생산 일치.
2. track 연결이 TrackRef/생산 association으로 정확하고, 단순 최근접 ΔR 연결로 대체하지 않음.
3. 원본 covariance와 실제 compressed covariance의 직접 대조 및 packing library round-trip 일치.
4. 세 daughter의 independent truth 상태와 부정확/누락 association을 모두 기록.
5. 원본 covariance/packed covariance 외 조건을 고정한 paired fit; 중심/폭/잔차/성공률을 함께 기록.
6. 실제 대상-bin 후보 수와 저장·CPU 비용을 보고. 후보가 너무 적으면 pilot을 통계적 결론으로 과장하지 않음.

#### 비용 근거 및 중단 지점

기존 `dstar_fullchain_benchmark_20260923/REPORT.md`의 100-event 측정: prompt 35.21 CPU s/accepted event, nonprompt 43.12 s/event. 따라서 1,000+1,000 pilot의 기존 체인 계산량은 **약 21.8 core-hours**. Initialization, 새 진단/association, I/O와 재시도 비용은 별도이며 달력시간 약속이 아니다. 최대 RSS 기존 측정은 약 4.23 GiB.

기존 파일 크기로 MiniAOD 약 0.59 GB, RECO 약 2.22 GB가 baseline이다. 추가 TrackingParticle/association·원본 covariance 보존 크기는 아직 측정되지 않았다. 처음 10 events의 실제 산출 크기를 확인하고 전체 pilot 저장 계획을 확정한다. 기존 입력을 덮어쓰거나 자동 정리하지 않는다.

이 승인은 **pilot 2,000 accepted events까지만**이다. 이는 최종 FWHM 차이의 통계적 확정을 위한 대량 MC 승인과 다르다. Pilot 종료 후 대상-bin yield, common support와 paired-effect precision을 근거로 다음 규모를 별도 제안한다. 검증 실패 또는 identity/truth 누락이면 문제를 기록하고 확대하지 않는다.

#### 최종 인과 판별

- 원본 covariance로 바꿨을 때 truth가 맞는 동일 후보의 중심집중/BDT contrast가 안정적으로 사라지면 covariance reconstruction 가설을 지지한다.
- 두 covariance에서 같은 변화가 남고 topology를 충분히 맞췄을 때 감소하면 선택×vertex geometry 가설을 지지한다.
- independent truth를 적용하면 사라지면 잘못된 slow-track association 가설을 지지한다.
- 독립 표본에서 반복되지 않으면 통계변동/shape estimator 효과를 재평가한다.

어느 경우에도 PSD라는 이유만으로 uncertainty coverage가 보장된다고 판단하지 않는다.
<!-- source:SRC-10:end -->

<a id="src-11"></a>

## SRC-11 — bdt_refit_causality_20260929/PILOT_COVARIANCE_REPORT.md

**기록 구분:** 조사 보고서·해석 (표본·시점별 결과). **원문:** [bdt_refit_causality_20260929/PILOT_COVARIANCE_REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/PILOT_COVARIANCE_REPORT.md). **SHA256:** `b147ba9d527f4ae9b4e0e84627dcb0a2862e37525f17596fdbdbec61cdc63f18`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-11:start -->
### Completed pilot: original versus packed track covariance

Date: 2026-09-29. CERN HTCondor cluster 12717346: all 20 jobs completed; 1,000 PR + 1,000 NPR stored events. This audit uses the generated sample, without further production or changes to canonical production code.

#### Confirmed result

The covariance representation used by the current MiniAOD packed-track refit can lose positive semidefiniteness relative to the same original RECO track. This is a problem with the packed covariance and its use in this refit; it is not a conclusion that all MiniAOD observables or analyses are invalid.

All 108 geometrically GEN-matched D* candidates (54 PR, 54 NPR) have exact original/packed track associations for all three daughters. The original matrices are positive definite for all 324 candidate-track instances. The packed matrices fail the positive-semidefinite requirement as follows:

| Tracks | Instances | Original indefinite | Packed indefinite |
|---|---:|---:|---:|
| PR slow pion | 54 | 0 | 34 |
| NPR slow pion | 54 | 0 | 26 |
| All slow pions | 108 | 0 | 60 (55.6%) |
| PR D0 daughters | 108 | 0 | 6 |
| NPR D0 daughters | 108 | 0 | 4 |
| All D0 daughters | 216 | 0 | 10 (4.63%) |

There are 108 unique slow tracks and 214 unique D0 daughter tracks; the unique-track indefinite counts remain 60 and 10. At least one packed daughter matrix is indefinite in 63/108 candidates. These rates describe this diagnostic sample, not the full production population.

#### Why this is a covariance defect

For any real vector a, a covariance matrix must satisfy a^T C a = Var(a^T X) >= 0. Consequently each normalized correlation has magnitude at most one. We check finite, symmetric 5x5 matrices with positive diagonals, form R_ij = C_ij/sqrt(C_ii C_jj), and flag min eigenvalue(R) < -1e-10. Diagonal rescaling preserves the signs of the eigenvalues (inertia).

In NPR_119, run 929119, lumi 3, event 2035, candidate 8, original track key 2056, schema 520, the lambda–dsz correlation changes from -0.972577 to -3.706244. Its minimum correlation-matrix eigenvalue changes from +0.0250644 to -2.707037. This is far larger than numerical roundoff. This candidate has pT=7.516 GeV/c, y=1.0967, centrality bin 7 and BDT=0.99898, and is outside the target rapidity bin.

| Matrix entry | Original RECO | Packed |
|---|---:|---:|
| C11 | 3.87861155e-6 | 3.80364156e-7 |
| C44 | 1.37274867e-4 | 1.36240560e-4 |
| C14 | -2.24418000e-5 | -2.66800707e-5 |

Positive definiteness of the original matrix alone does not demonstrate calibrated statistical uncertainties or coverage.

#### Controlled impact on the fitted mass

Each candidate is refitted using five modes: packed baseline; identical state/covariance reconstructed as a control; original slow-pion covariance only; all three original covariance matrices; and all three full original tracks. The covariance-only modes keep packed track momenta/reference states fixed. All 540 mode/candidate combinations return valid D0 and D* fits.

Define delta m = fitted D* mass minus fitted D0 child mass. The shifts below are alternative minus packed, in MeV.

| Intervention | Median absolute shift | Signed shift range | Absolute shift >0.1 MeV |
|---|---:|---:|---:|
| Identity control | 0 | -3.55e-12 to +1.07e-11 | 0/108 |
| Original slow covariance only | 0.03862 | -1.34278 to +0.64296 | 24/108 |
| All original covariances | 0.04023 | -1.34011 to +0.64169 | 27/108 |
| All original tracks | 0.04156 | -1.34647 to +0.64873 | 27/108 |

The slow-only >0.1 MeV counts are PR14/54 and NPR10/54. All 24 remain in the 105-candidate paired fitted-mass window (140<=delta m<153 MeV and 1.81120<=m(D0)<1.91738 GeV). This window is not the nominal full selection.

For PR_109, run 929109, lumi 4, event 3275, candidate 0, restoring only slow covariance changes delta m from 146.78980 to 145.44702 MeV. This candidate is outside the target rapidity/centrality selection. The independent packed replay agrees with the stored refitted slow state to at most 1.86e-9 across its components; that largest component is a position coordinate in cm.

A >0.1 MeV shift occurs in 19/60 candidates with an indefinite packed slow matrix, and 5/48 whose packed slow matrix passes the PSD test. Thus loss of PSD is a proven defect, but it is not the sole diagnostic of approximation-induced fit changes. A valid fitter return also does not certify valid input covariance.

#### Interpretation and open test

Confirmed: original-to-packed covariance changes include mathematically invalid matrices, and changing only slow-pion covariance changes the D* mass difference. The effect is not explained by reconstructing an identical track object or numerical closure noise.

Not established: how much this explains the particular BDT-dependent prompt/nonprompt FWHM difference at pT7–10, |y|<0.3, centrality0–10%, tracker |cos(theta*)|0.6–0.8 and DCA0–0.08 cm. The new pilot has zero candidates already after the pT/y/centrality selection. It cannot measure that target distribution or its change with BDT.

The specific hypothesis is that BDT selects different slow-track covariance/kinematic populations, whose vertex-refit responses distort the conditional delta-m shape. To test it, use paired original/packed candidates in the target and neighboring cos bins, freeze candidate membership and BDT labels first, compare delta-m residuals and widths under slow-covariance-only restoration, then separately evaluate selection migration. Truth residuals/pulls are needed to assess resolution and uncertainty calibration. Merely forcing a matrix positive definite is not a validated recovery of the original covariance. No additional production has been launched by this audit.

#### Implementation evidence

[CMSSW_13_2_16_patch1 PackedCandidate.cc, packing/unpacking](https://github.com/cms-sw/cmssw/blob/CMSSW_13_2_16_patch1/DataFormats/PatCandidates/src/PackedCandidate.cc#L74-L114) stores selected covariance components and reconstructs the matrix. Its [pseudoPosDefTrack implementation](https://github.com/cms-sw/cmssw/blob/CMSSW_13_2_16_patch1/DataFormats/PatCandidates/src/PackedCandidate.cc#L155-L198) explicitly says “if not positive-definite, alter values to allow for pos-def”. This source documents the possibility and a numerical regularization mechanism; it does not establish that regularization restores the original uncertainties or solves this analysis's FWHM issue.

#### Reproduction and artifacts

Run `OPENBLAS_NUM_THREADS=1 python3 audit_pilot_covariance.py` from this study directory. Inputs: `parallel_pilot_complete_records.json` (20 job validation records and 108 candidate records). Outputs: `pilot_covariance_summary.json` and `pilot_covariance_candidates.csv`. Assertions check event identities, candidate identities, finite symmetric matrices, positive diagonal elements and repeated-track consistency. Exact counts and per-candidate mode differences are preserved in the JSON/CSV.

Remote study: `/eos/home-j/junseok/analysis/dstarana/VertexCompositeStudies/studies/refit_diagnostics_20260918/bdt_refit_causality_20260929`. Source ROOT outputs: `parallel_pilot/{PR_101..PR_110,NPR_111..NPR_120}/`; source paired records and validation receipts: `/afs/cern.ch/user/j/junseok/private/bdt_refit_parallel_20260929/jobs/*/results/`.
<!-- source:SRC-11:end -->

<a id="src-12"></a>

## SRC-12 — bdt_refit_causality_20260929/REPORT.md

**기록 구분:** 조사 보고서·해석 (표본·시점별 결과). **원문:** [bdt_refit_causality_20260929/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/REPORT.md). **SHA256:** `592d73708de246b8bec8551b545794ec00f2b47adc12ca337d2af617e5fd3625`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-12:start -->
### BDT selection and D-star vertex-refit study — 2026-09-29

<!-- embedded-truth-latest:start -->
**최신 생성점 복원 진단: [embedded_truth/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/embedded_truth/REPORT.md).**

- 기존 MiniAOD의 배경 GEN ancestry와 실제 embedding 소스로 signal 생성점을 복원하고, 같은 기준점에서 입력·fitted slow pion을 비교했다. 독립적으로 저장된 simulation vertex와의 직접 대조는 아니며, reco PV를 truth로 대체하지 않았다.
- 검증된 후보 1229개, 작업 61/63개 완료. 미수락 작업: NPR_15, NPR_24. Prompt 716개는 모두 포함. 이전 단계에서 누락된 NPR_26의 17개는 별도이며 이번 실행에도 포함되지 않는다. 상세 실행 상태와 원격 입력 오류는 records.json 및 REPORT.md에 기록한다.
- Prompt BDT 통과 491개: 같은 생성점 기준 평균 방향 오차 ΔR = 0.005580 → 0.004837. Fitted vertex 위치 오차 중앙값 1.146 mm, GEN 비행 방향에 수직인 성분 중앙값 24.1 μm.
- 같은 fitted state의 운동량 평가점을 바꾸면 중앙-bin 비율 19.791% → 13.405%. 원래 질량 잔차 width68 = 1.3045 → 1.4486 MeV; 공통 생성점에서는 1.3044 → 1.3702 MeV. 좁은 fitted core를 전체 질량 정확도 개선으로 볼 수 없다.
- 해석: fitted vertex의 위치에 따른 상대각 변화가 질량 모양에 크게 작용한다. 이는 전체 reconstruction의 오류/정당성, BDT의 유일한 원인, covariance coverage 또는 yield closure의 확정과는 다르다. GEN 기준점은 DATA용 처치가 아니다. Production 소스 해시 유지.

아래 블록은 이전 시점의 기록이다. 생성점 정보를 전혀 이용할 수 없다는 이전 제한은 이번 source-derived 복원으로 진전됐으며, 독립 simulation-vertex 대조의 한계는 남는다.
<!-- embedded-truth-latest:end -->

<!-- geometry-selection-latest:start -->
**Latest signed-geometry follow-up:** [geometry_selection/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/geometry_selection/REPORT.md). Pre-fit input-line geometry predicts the propagation Delta-m sign for96.5% of weighted prompt BDT-pass candidates (95.1%fail); slow-pion phi-only rotation reproduces the full propagation mass shift within6.32e-6MeV. Nearly parallel D0/slow-pion trajectories give a weak longitudinal common-vertex constraint. Prompt pass has50central-bin entrants and33exits during propagation, but only46.8%move toward the center; core gain is not overall resolution improvement. No unique BDT-topology cause or significant BDT-specific interaction is established. Actual SIM provenance and one MiniAOD event support unsmeared signal GEN stored alongside translated background GEN; exact per-event embedding-point validation remains unresolved, so no angular-truth or yield-closure claim. All1264available candidates, five plots, matching diagnostics and validation are saved; production unchanged.
<!-- geometry-selection-latest:end -->


<!-- truth-geometry-latest:start -->
**Latest completed diagnosis:** [truth_geometry/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/truth_geometry/REPORT.md). Same-candidate geometry validated for1,264/1,281 targets (all716prompt;548/565nonprompt),63/64jobs. The remaining17nonprompt targets are excluded because the FNAL input repeatedly timed out. For all491prompt BDT-pass candidates, internal central-bin fractions are15.259%→18.761%→19.791% for input→propagated input→fitted states, with the final fitted vertex held fixed. This numerical decomposition does not establish an independent causal role of propagation. GEN mass-residual width68 is1.3045→1.4497MeV; posdef intervention leaves it1.4497→1.4502MeV. The prompt GEN coordinate frame remains unvalidated; a checked MiniAOD event has zero signal GEN origin and empty hit-based association products. No BDT-error or covariance-defect cause is established. Details, fixed-definition comparisons, five plots, job failures and remaining checks are in the linked report. Earlier sections below retain their historical scope.
<!-- truth-geometry-latest:end -->

**Latest follow-up:** see the appended follow-up diagnosis and `FOLLOWUP_REPORT.md`; the original sections below are the earlier stage record.

#### Result and evidence level

Confirmed: in a fresh production replay of 24 frozen targets, applying the D0 producer BDT cut at 0.95 removes the 12 below-threshold targets and leaves all 31 checked quantities of the 12 surviving targets exactly unchanged. Thus this check finds selection of different candidates, rather than a change of the reconstruction of the surviving candidates.

Measured in the matched official-pT4 subset: the prompt BDT-pass sample gains 4.53 percentage points in a fixed central Delta-m bin during the D-star refit; the prompt BDT-fail sample gains -0.44 points. The corresponding nonprompt gains are 4.49 and 4.61 points. Restoring the original slow-pion direction largely removes the central concentration in the prompt pass sample while holding the D0 state fixed.

Interpretation: BDT-selected D0 topology and the slow-pion angular response of the subsequent refit provide a mechanism for a changed D-star central peak even when the daughter D0 mass shape changes little. This is a measured association and a kinematic decomposition, not proof of the unique underlying physical or numerical cause. The prompt pass-minus-fail difference is 4.98 +/- 3.16 percentage points in this subset.

#### Exact scope and identity

- Reconstructed D-star pT 7–10 GeV/c, |y|<0.3, centrality 0–10%, DCA 0–0.08 cm.
- Nominal tracker-axis |cos(theta*)| 0.6–0.8; candidate membership and angular-bin assignment held fixed between reconstruction stages.
- Truth-matched, non-swap candidates from the September 28 campaign, with loose BDT>=0; pass BDT>=0.95 and fail 0<=BDT<0.95.
- Delta-m 140–153 MeV and daughter D0 mass 1811.20–1917.38 MeV retained as in the baseline.
- Official PR pT4 and official NPR pT4 only: 716 prompt and 565 nonprompt candidates. Private MC and higher source thresholds are outside this replay scope.
- 1,281/1,281 matched candidates from 877 diagnostic ROOT files; no missing or ambiguous matches. Identity includes original LFN, run/lumi/event and stored candidate kinematics.
- Current campaign physical weights copied once and kept fixed. Source-task indices were mapped through the actual official friend-task manifest, not interpreted as CRAB job numbers.
- Existing diagnostic outputs supply the full 1,281-candidate comparison. Fresh MiniAOD re-production covers a frozen stratified sample of 24; these counts must not be conflated.

Manifest: `/home/CMS/junseok/DStarAna/Data/Weights/OfficialPrivate3M5M_20260928/campaign/manifests/official_friend_tasks.json`.
Loose candidates: sibling study `prompt_mass_cos06_08_20260929/pt7_10/mva_comparison/candidates.npz`.
Exact joins and receipts: `join_validation.json`, `candidate_manifest.json`, `blocks/`, `paired_candidates.npz`.

#### Before/after central concentration

The measured central bin is fixed at 145.28813559322035 <= Delta-m < 145.5084745762712 MeV. Values are weighted fractions of each selected sample, not fitted FWHM.

| Sample | BDT group | N | Before D-star refit | After D-star refit | Change (percentage points) |
|---|---|---:|---:|---:|---:|
| Prompt | >=0.95 | 491 | 15.259% | 19.791% | +4.532 |
| Prompt | [0,0.95) | 225 | 16.000% | 15.556% | -0.444 |
| Nonprompt | >=0.95 | 388 | 11.288% | 15.781% | +4.494 |
| Nonprompt | [0,0.95) | 177 | 11.028% | 15.642% | +4.615 |

The pass-minus-fail difference of these changes is prompt 4.9765 +/- 3.1603 points and nonprompt -0.1210 +/- 3.7410 points. Uncertainties are the standard deviation of 1,000 event-cluster Poisson bootstrap replicas with shared draws for the paired stages and nested selections (seed 20260929). Weights and selection definitions are fixed; no model or source-family uncertainty is included.

Prompt candidates that enter this central bin through refitting have a 74.31% weighted BDT-pass efficiency, compared with 61.28% for those leaving it. Nonprompt efficiencies are 69.77% and 70.83%, respectively. These are conditional efficiencies in the observed sample; finite-sample fluctuations remain relevant.

The central-bin effect is not a statement that the full resolution improves. Prompt pass Q84-Q16 changes from 1.3045 to 1.4486 MeV. Broadening the central window to +/-0.66 MeV gives almost identical prompt pass/fail refit changes (-5.77/-5.78 points). Full window dependence is in `window_sensitivity.csv`.

![Paired production states](/home/jun502s/DstarAna/DStarAnalysis/Fit/PbPb/Test/SpinAlignment/output/campaign_26Sep28_official_private3M5M_source_family_1to1/study/bdt_refit_causality_20260929/bdt_before_after.png)

#### Kinematic decomposition

Define:
- before = M(D0Internal + SlowOriginal) - M(D0Internal);
- final = M(D0Refit + SlowRefit) - M(D0Refit);
- fixed-D0 = M(D0Internal + SlowRefit) - M(D0Internal);
- restored direction: internal D0 held fixed; slow-pion fitted pT and mass kept, but eta and phi restored to their original values.

For prompt BDT>=0.95, central fractions are:
- before: 15.259%;
- final: 19.791%;
- fixed D0 with fitted slow pion: 19.585%;
- fixed D0 with original slow eta and phi: 15.671%.

The fitted-versus-restored slow-direction contribution with the same fixed D0 is 3.914 +/- 1.847 percentage points. This explicitly localizes much of the observed central-bin migration to slow-pion angular changes, without requiring a comparable change of the D0 invariant mass.

Restoring eta only gives 16.304%, and phi only 14.214%. These correlated nonlinear substitutions are not additive contributions, and neither is a replacement physical fit. They do not establish that phi alone is a defect. For nonprompt pass, restoring both angles gives 11.288% versus 15.781% with fitted slow momentum.

The exact invariant-mass identity is:
`M(D0+pi)^2 = m(D0)^2 + m(pi)^2 + 2 [E(D0)E(pi) - p(D0) dot p(pi)]`.
Consequently, unchanged daughter invariant mass does not imply unchanged D0–slow-pion opening angle or parent mass.

Official CMS documentation supports the scope of a vertex fit, not this sample-specific diagnosis: [Vertex Fitting, opening paragraph](https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideVertexFitting) includes “constrained track parameters and their covariances”. The [Kinematic Vertex Fit guide, KinematicParticle and KinematicVertex section](https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideKinematicVertexFit) describes the seven-component position, momentum and mass state with its joint covariance. Our sample-specific conclusions come from the paired outputs above.

#### BDT topology and covariance checks

Prompt BDT pass/fail weighted averages in this sample:
- D0 3D pointing angle: 0.03395 / 0.10460 rad.
- D0 decay-length significance: 9.7425 / 4.4556.
- D0 vertex probability: 0.53178 / 0.42564.
- Original slow-pion pT: 0.62098 / 0.60352 GeV.
- Original slow-pion pT error: 0.008057 / 0.008206 GeV.
- Fraction with an indefinite original slow-track covariance: 66.392% / 65.778%.

The BDT selects distinctly different D0 topologies; it does not simply increase the fraction of indefinite slow-track covariances in this prompt subset. Both covariance groups show effects. This rules out claiming that a changed indefinite-covariance fraction alone explains the BDT dependence. It does not establish that those covariances are harmless.

The production ONNX input vector has 20 entries:
`pT, y, centrality, VtxProb, 3DCosPointingAngle, 3DPointingAngle, 2DCosPointingAngle, 2DPointingAngle, 3DDecayLength, 3DDecayLengthSignificance, 2DDecayLength, 2DDecayLengthSignificance, pTD1, EtaD1, pTerrD1, pTD2, EtaD2, pTerrD2, Trk3DDCA, dEta_dau`.
Model file: `XGBoost_Model_OnlyNonPrompt_05Mar26_Centrality_pTerr_ptErr011_1.onnx`. The filename alone is not verification of training provenance. The fresh replay saved the actual input vector and reproduced the original score exactly for every target.

Individual BDT-variable causation is unresolved. Marginal mean differences are not a controlled causal isolation of one input.

#### Actual CMSSW re-production and interventions

Execution host: gpusystem -> junseok@lxplus9114.cern.ch.
Study runtime: `/eos/home-j/junseok/analysis/dstarana/VertexCompositeStudies/studies/refit_diagnostics_20260918/bdt_refit_causality_20260929/runtime/CMSSW_13_2_11`, architecture el8_amd64_gcc11.
Valid proxy was read from the user's renewed cms-auth proxy path; no credentials are included in artifacts.

Preserved production/probe sources were copied from:
`/eos/home-j/junseok/analysis/dstarana/VertexCompositeStudies/source_variants/refit_diagnostics/`.
The new runtime was built in the EL8 CMSSW container with two build threads. No ROOT macro compilation was used.

Source-hash comparison covers 165 files, with only two diagnostic additions:
1. `VertexCompositeAnalysis/VertexCompositeProducer/src/D0Fitter.cc`: save the actual 20 BDT inputs using the existing transient diagnostic helper.
2. `RefitAudit/Probe/plugins/CausalFollowupProbe.cc`: output the BDT vector/score and relevant candidate states.

No canonical production source or original production result was modified. Exact changes: `diagnostic_only.diff`. Full preserved/runtime hashes: remote `runtime_provenance.json`.

Frozen targets: PR12 + NPR12, stratified by BDT pass/fail, covariance group and migration category. This intentional sample is not suitable for estimating population frequencies.

Baseline replay: eight batches from original MiniAOD LFNs; all exit 0. All 24 candidate BDT scores exactly match the campaign and all checked diagnostic four-vectors agree to a maximum absolute difference of 1.5987e-14. Independent refit replays are valid and agree within the checked tolerance. Twelve selected candidates exercise the pre-existing general inverse path; all inverse operations complete. Success of that operation alone is not a covariance-quality validation.

Direct producer-cut intervention: set `process.generalD0CandidatesNew.mvaCut=cms.double(0.95)` for the same inputs. Match candidates by original kaon/pion/slow-pion track keys, not by shifted candidate indices. Twelve expected survivors remain and twelve expected failures disappear. All 31 checked survivor quantities are exactly unchanged (maximum difference 0), including D-star/D0 masses, score, slow-pion momentum and internal/final refit four-vectors.

Execution anomaly recorded, not hidden: seven initial tight batches exit 0. `tight_NPR_02`, whose three target events have no surviving D0 candidate, finishes its event reports and then aborts during shutdown (exit 134). An explicit repeat with the same physics configuration/input and without the unnecessary diagnostic inverse-trace LD_PRELOAD exits 0 and is the output used for that batch's selection validation. The shutdown failure's root cause has not been proven; the successful repeat does not by itself identify the library as its cause. Original and repeat logs/FJR/output are retained, and both statuses are recorded in `tight_cut_validation.json`.

Study setup failures also retained: duplicate process history when first testing an existing event copy, and a diagnostic user-data dictionary issue before switching to the existing transient helper. Final results use original MiniAOD and the verified additive diagnostic implementation.

#### Remaining questions and stopping point

Completed: exact joins, paired state analysis, original-MiniAOD reproduction, BDT-input/score validation, and an actual producer-cut intervention. The observed association is localized to slow-pion angular refit response in this bin.

Not established:
- a unique BDT input that causes the effect;
- whether the angular response is a correct covariance-weighted physical constraint or is distorted by MiniAOD covariance representation;
- a statistically conclusive prompt-specific effect in this official-pT4 subset;
- a quantitative explanation of the full official+private campaign FWHM change, other cosine bins, or higher-threshold source families.

Full RECO/pre-packing covariance and independent track-level truth would address the covariance interpretation. A controlled topology/kinematics-matched comparison and broader source statistics are needed to separate selection correlations and quantify the full-campaign contribution. No covariance clipping, physical refit replacement, or production treatment is justified by this diagnostic alone.

#### Artifacts and reproduction

Local directory: `/home/jun502s/DstarAna/DStarAnalysis/Fit/PbPb/Test/SpinAlignment/output/campaign_26Sep28_official_private3M5M_source_family_1to1/study/bdt_refit_causality_20260929`.
Remote directory: `/eos/home-j/junseok/analysis/dstarana/VertexCompositeStudies/studies/refit_diagnostics_20260918/bdt_refit_causality_20260929`.

Local numeric/figure artifacts:
`analysis.json`, `stage_metrics.csv`, `transition_efficiencies.csv`, `paired_bootstrap.csv`, `direction_components.csv`, `window_sensitivity.csv`, `feature_metrics.csv`, `bdt_before_after.png`, `bdt_before_after.pdf`.
Validation: `join_validation.json`, `replay_validation.json`, `tight_cut_validation.json`.
Code: `join_diagnostics.py`, `analyze_pairs.py`, `plot_pairs.py`, `verify_replays.py`, and remote-oriented `verify_tight.py`.

Remote runtime/config/log artifacts:
`runtime_provenance.json`, `diagnostic_only.diff`, `replay_targets.json`, `jobs.json`, `configs/`, `run_replays.py`, `run_tight.py`, `replay_status.json`, `tight_status.json`, `work/`.
Run `verify_tight.py` in the remote study directory with uproot/awkward available; it explicitly records the successful uninstrumented repeat.

<!-- FOLLOWUP_20260929 -->

### 원인 진단 후속 리포트 — 2026-09-29

#### 최종 판단

이번 단계에서 관측된 피크 중심 집중의 직접적인 운동학적 경로는 **BDT가 선택하는 D0 후보 집단과 D* vertex refit의 slow-pion 방향 변화의 결합**으로 좁혀졌다. 다만 이것을 특정 BDT 입력 하나 또는 특정 수치적 결함 하나의 인과효과로 확정할 수는 없다. 공식 pT4 표본의 cos 0.6–0.8에서 pass−fail 차이는 4.98 ± 3.34 percentage points(pp)이며, 다섯 cos × 두 family를 함께 고려한 95% 동시 구간도 0을 포함한다. 통계적 변동 가능성은 남아 있다.

별도로 **정상 RECO covariance가 MiniAOD 방식의 압축·복원으로 indefinite가 되는 경로는 통제 실험에서 직접 재현했다.** 그러나 실제 문제 표본의 24개 후보를 재생산하여 covariance를 바꿔 본 결과, CMSSW의 `pseudoPosDefTrack()` 처리는 중앙-bin 소속을 하나도 바꾸지 않았다. 그러므로 covariance가 잘못된 확률오차 행렬이라는 사실과, 이것이 이번 BDT-dependent peak의 주원인이라는 주장은 서로 다르다.

현재 결론은 원인의 전부를 해결했다는 뜻이 아니다. 작은 자료 재분석·재생산으로 할 수 있는 분리는 완료했고, 남은 결정적 검증에는 **동일한 D* daughter track의 packing 전 covariance와 독립 TrackingParticle truth를 함께 보존한 표본**이 필요하다. 검증되지 않은 covariance 처리를 nominal production에 적용하지 않았다.

#### 진행 체크리스트

- [x] 기존 production/vertex-refit 및 181-candidate causal follow-up 리포트 확인.
- [x] official PR/NPR pT4의 모든 다섯 cos 구간 6,159/6,159 후보 연결. 6,155개는 기존 진단 출력, 4개는 원본 MiniAOD 재생산으로 복구. 원래 core-bin 1,281개 값은 모두 정확히 동일.
- [x] source LFN·run/lumi/event·후보 운동학 및 Δm 일치 확인; 누락 0, 연결 오류 0.
- [x] BDT 0.95 producer cut 직접 개입: 24개 중 12개 제거, 남은 12개 31개 물리/진단 값 동일.
- [x] 5,747 event cluster, 5,000-replica bootstrap으로 모든 cos의 paired refit gain 및 동시 구간 계산.
- [x] D0 state / slow pT / η / φ 16개 조합, 24개 변경 순서 평균으로 성분 기여 계산; 합이 실제 변화와 수치적으로 일치.
- [x] 운동학·D0 topology·covariance 조건 맞춤, common support 및 잔여 표준화 평균차(SMD) 점검.
- [x] 가중치, source 혼합, GEN/slow-track 중복, 원래 GEN matching 경계, score 구간, peak window 폭·중심 이동 점검.
- [x] 전체 official+private 15,706개 loose 후보에서 source 분리 및 KDE FWHM 민감도 점검.
- [x] 실제 MiniAOD 24개 대상 × identity/4개 covariance 변경 = 120개 독립 D* refit 비교. 8/8 batch exit 0, 모두 valid; identity slow 4-vector 차이 0.
- [x] 별도 보존 RECO 20 events의 12,060 low-pT track에서 압축·복원 통제 실험. 원본/sparse-only/schema 8/schema 520 비교 완료.
- [x] 표·그림·실행 코드·로그·소스 hash·제한점과 승인 필요 작업을 기록.
- [ ] 문제 D* daughter 자체의 packing 전 원래 covariance를 이용한 paired refit: 해당 official 이벤트의 full RECO가 없어 미실시.
- [ ] 독립 track truth에 의한 잘못된 slow-pion 연결 배제 및 resolution pull 검증: 현재 문제 MiniAOD의 association은 비어 있어 미실시.
- [ ] 새 full-chain production/대량 MC 생성: 미제출, 별도 승인 필요.

위 완료 기준은 이 단계의 유한한 진단 범위다. 모든 가능한 물리 검증이나 최종 uncertainty validation을 완료했다는 뜻은 아니다.

#### 표본 및 정의

reco D* pT 7–10 GeV/c, |y|<0.3, centrality 0–10%, DCA 0–0.08 cm, nominal tracker |cosθ*|의 다섯 구간. truth-matched/non-swap, 기존 Δm 140–153 MeV 및 D0 mass 1811.20–1917.38 MeV 선택을 유지했다. paired state 비교는 official PR/NPR pT4만 사용한다. source/FWHM 비교만 전체 official+private source를 사용한다.

중앙 bin은 145.28813559322035 ≤ Δm < 145.5084745762712 MeV로 고정했다. `gain`은 같은 후보 집단에서 refit 후 중앙 비율 − refit 전 중앙 비율이다. `contrast`는 BDT≥0.95 gain − 0≤BDT<0.95 gain이다. 이는 fitted FWHM, 유의도, detector resolution과 같은 양이 아니다.

후보와 nominal cosine/weight는 reconstruction stage 사이에서 고정했다. weight는 현재 campaign 값을 한 번 사용했다. event cluster는 source/run/lumi/event로 정의했다. 원래 1,000-replica 보고와 후속 5,000-replica 결과의 오차 소수점 차이는 bootstrap 반복 수 및 전체 event 집합이 바뀐 데 따른 Monte Carlo 차이이며, 중심값 정의는 바뀌지 않았다.

#### 모든 cos에서의 비교


| Family | |cosθ*| | Pass−fail gain (pp) | 동시 95% 구간 반폭 (pp) |
|---|---|---|---|
| PR | 0.0–0.2 | -1.75 ± 2.95 | 8.36 |
| PR | 0.2–0.4 | -3.22 ± 3.02 | 8.55 |
| PR | 0.4–0.6 | 2.28 ± 3.48 | 9.87 |
| PR | 0.6–0.8 | 4.98 ± 3.34 | 9.46 |
| PR | 0.8–1.0 | -2.63 ± 3.46 | 9.81 |
| NPR | 0.0–0.2 | -4.51 ± 3.62 | 10.26 |
| NPR | 0.2–0.4 | -1.63 ± 4.07 | 11.54 |
| NPR | 0.4–0.6 | -4.27 ± 3.87 | 10.98 |
| NPR | 0.6–0.8 | -0.12 ± 3.84 | 10.88 |
| NPR | 0.8–1.0 | -0.53 ± 3.97 | 11.26 |


10개 contrast의 bootstrap maximum absolute standardized deviation으로 동시 반폭을 정했다. 어느 구간도 이 비교에서 0을 배제하지 않는다. 여러 window/score 조건은 탐색적 진단이며 독립적인 discovery test로 사용하지 않는다. cos 0.6–0.8의 점추정이 다른 구간보다 커 보이는 것은 확인되지만, 특수한 물리적 이상이 통계적으로 입증된 것은 아니다.

#### 어떤 상태 변화가 피크를 움직이는가

Prompt cos 0.6–0.8, BDT≥0.95의 실제 gain +4.532 pp를 네 성분에 분배했다. 각 성분을 바꾸는 모든 순서를 평균한 Shapley allocation으로 순서 의존성을 제거했다.


| 성분 | 중앙 gain 기여 (pp) |
|---|---|
| D0_state | 0.017 ± 0.109 |
| slow_pT | 0.258 ± 0.193 |
| slow_eta | 1.033 ± 1.065 |
| slow_phi | 3.225 ± 1.587 |


η+φ 기여의 점추정 합은 4.257 pp로 전체 gain의 약 94%다. 이 수치는 해당 표본·중앙구간에 대한 운동학적 분해이며, 통계적으로 확정한 인과 기여율이 아니다. 성분 오차는 상관되어 있으므로 표의 오차를 독립적으로 합하지 않는다. D0 state와 slow pT 기여는 이 진단에서 작다. 단, 일부 후보의 D0 mass 변화까지 모두 작다는 주장은 하지 않는다.

수학적으로 `M(D0+π)^2 = mD0² + mπ² + 2(ED0 Eπ − pD0·pπ)`이므로, daughter D0 invariant mass가 비슷해도 상대 운동량 방향이 바뀌면 D* Δm은 바뀐다. CMS [Vertex Fitting 안내, 도입부](https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideVertexFitting)의 “constrained track parameters and their covariances”가 이 일반적 설명의 근거다. 이 공식 문서가 이번 표본의 특수한 BDT 의존성까지 검증해 주는 것은 아니다.

#### 운동학과 topology를 맞춘 비교

결과를 보지 않고 각 변수의 weighted quantile cell을 구성하고, pass와 fail 각각 원래 후보가 최소 5개인 cell만 사용했다. 두 집단의 cell 확률 중 작은 값을 공통 목표분포로 삼았다. bootstrap마다 cell 가중치를 재계산하고, cell 경계와 support 선택은 고정했다. 아래는 prompt cos 0.6–0.8이다.


| 조건 | 통과 후보 보존 | 같은 support, 조정 전 (pp) | 조정 후 (pp) | 조정 변수 최대 |SMD| |
|---|---|---|---|---|
| Dstar_kinematics | 74.4% | 4.21 | 3.11 ± 3.55 | 0.071 |
| slow_kinematics | 88.7% | 5.36 | 5.27 ± 3.57 | 0.082 |
| D0_topology | 18.3% | 6.47 | 5.54 ± 6.44 | 0.577 |
| D0_pointing_only | 100.0% | 4.98 | 4.87 ± 4.24 | 0.367 |
| D0_decay_only | 100.0% | 4.98 | 4.76 ± 4.33 | 0.173 |
| covariance | 90.3% | 3.40 | 2.21 ± 3.28 | 0.068 |
| joint_coarse | 36.5% | 4.88 | 5.02 ± 5.36 | 0.788 |


D* 운동학 조정은 contrast를 4.21→3.11 pp로 낮추지만 오차가 크다. slow-pion 운동학만 맞추어도 차이는 사라지지 않는다. 이것은 해당 변수들이 무관하다는 증명이 아니다.

D0 topology 세 변수를 동시에 맞추면 pass 후보의 18.3%만 남고, 잔여 최대 |SMD|도 0.577로 크다. 즉 **원인 분리를 위한 비교 조건이 충분히 맞지 않았다.** 이런 결과를 보고 특정 topology 입력이 원인이라고 하거나, 반대로 topology 원인이 배제됐다고 결론내릴 수 없다. joint coarse 조건도 같은 제한이 있다. pointing/decay 단독 coarse matching 역시 잔여 불균형이 남는다.

#### 간단한 대안 설명 점검


| 검사 | Prompt core contrast (pp) |
|---|---|
| baseline | 4.98 ± 3.34 |
| unit_weights | 4.93 ± 3.32 |
| GEN_multiplicity_weight | 5.53 ± 3.44 |
| slow_multiplicity_weight | 5.10 ± 3.35 |
| original_GEN_dR_lt_0p03 | 5.03 ± 3.36 |
| both_GEN_dR_lt_0p01 | 7.26 ± 3.97 |
| PSD_only | 8.30 ± 4.99 |
| indefinite_only | 3.23 ± 4.33 |


GEN/slow 중복 multiplicity는 모든 다섯 cos를 합친 loose 표본에서 정의했다. 따라서 core만 사용한 이전 중복 보정과 값이 약간 다르다. 이들은 진단 가중치이며 production weight를 변경하지 않았다. 원래 slow track이 같은 accepted GEN에 대해 ΔR<0.03을 통과하지 못한 core 후보는 pass 5개/fail 1개이며, 그중 중앙 유입은 0개다. 따라서 이 단순 matching 경계 이동은 관측된 중앙 유입을 설명하지 않는다. fitted-based geometric matching의 전체 bias나 다른 track의 잘못된 연결까지 배제한 것은 아니다.

source 혼합 분해는 `tight−loose = within-source 변화 + source 비율 변화`의 정확한 대수적 항등식이다. 전체 official+private core prompt의 중앙 비율 변화는 +1.150 pp이며, source 비율 변화 항은 +0.027 pp, within-source 항은 +1.123 pp다. 따라서 단순 혼합 비율 변화가 이 중앙 비율 변화의 주원인이라는 설명은 지지되지 않는다. 이것은 source 내부 모델 차이를 전부 배제하는 검사는 아니다.

#### FWHM 숫자의 강건성

기존 DSCB FWHM 0.9095→0.6542 MeV와 달리 empirical width68은 1.4966→1.5212 MeV였다. 전체 분포가 일괄적으로 좁아진 상황과 맞지 않는다. 같은 nested 표본을 Gaussian KDE로 평활화하고 event-paired bootstrap 200회로 tight−loose FWHM 차이를 계산했다. KDE는 별도의 검출기 resolution model이 아니며 bandwidth를 0으로 외삽하지 않았다.


| Family | Bandwidth (MeV) | Loose → tight FWHM (MeV) | 차이 ± bootstrap SD (MeV) |
|---|---|---|---|
| PR | 0.1 | 0.925 → 0.816 | -0.109 ± 0.078 |
| PR | 0.15 | 1.055 → 0.978 | -0.077 ± 0.062 |
| PR | 0.2 | 1.171 → 1.121 | -0.050 ± 0.048 |
| PR | 0.3 | 1.382 → 1.358 | -0.024 ± 0.035 |
| NPR | 0.1 | 1.043 → 1.134 | 0.090 ± 0.071 |
| NPR | 0.15 | 1.114 → 1.177 | 0.064 ± 0.049 |
| NPR | 0.2 | 1.191 → 1.237 | 0.046 ± 0.039 |
| NPR | 0.3 | 1.366 → 1.393 | 0.026 ± 0.028 |


Prompt가 좁아지는 방향은 유지되지만 정도는 평활화에 민감하고 모든 표시 bandwidth의 bootstrap percentile 구간은 0을 포함한다. **특정 DSCB FWHM 감소율을 detector resolution 변화율로 해석하면 안 된다.** 이 검사만으로 기존 fit이 잘못됐다고 단정하는 것도 부당하다. fit FWHM 자체의 완전한 uncertainty/model study는 이번 비모수 비교와 구별한다.

#### Covariance 이상: 발생 경로와 피크 영향 분리

전체 6,159개 slow original covariance 중 3,924개가 indefinite다. core 1,281개에서는 λ–dsz 상관계수 |ρ14|>1이 612개, φ–dxy |ρ23|>1이 631개다(서로 중복 가능). 최소 normalized eigenvalue는 약 -1.43이고 |ρ|는 최대 약 2.43이다. 이는 단순 부동소수점 오차로 0을 조금 넘은 경우가 아니다. covariance의 필수 조건 `Cii*Cjj − Cij² >= 0`를 위반한다.

이번 24개 실제 MiniAOD 후보의 packed covariance version은 모두 1, schema는 모두 520이었다. [CMSSW 13_2_11 PackedCandidate.cc, packCovariance/unpackCovariance](https://github.com/cms-sw/cmssw/blob/CMSSW_13_2_11/DataFormats/PatCandidates/src/PackedCandidate.cc#L75-L115)는 다섯 diagonal과 세 off-diagonal만 저장·복원한다. [CovarianceParameterization.cc, pack/unpack](https://github.com/cms-sw/cmssw/blob/CMSSW_13_2_11/DataFormats/PatCandidates/src/CovarianceParameterization.cc#L168-L199)는 원소별 reference 비율을 압축한다.

실제 release 데이터파일의 schema 520에서 세 운동량 diagonal의 log quantizer `base`는 2, λ–dsz 및 φ–dxy는 8이었다. ROOT 설정 이름은 `bit`지만 [liblogintpack.h, pack16log/unpack16log](https://github.com/cms-sw/cmssw/blob/CMSSW_13_2_11/DataFormats/Math/interface/liblogintpack.h#L23-L65)의 인자는 `base`다. 이를 '2-bit 정밀도'라고 읽으면 안 된다. 원소를 각각 근사하므로 행렬 전체의 positive semidefiniteness가 자동으로 보장되지 않는다.

##### 실제 RECO track을 이용한 통제 round-trip

별도 private full-chain benchmark의 PR/NPR 각각 첫 10 events를 사용했다. highPurity, pixel hit 존재, 0.5≤pT<0.95 GeV, |η|<2.4인 실제 generalTracks를 pion 가정으로 PackedCandidate에 넣고 같은 CMSSW library로 압축·복원했다. 주로 underlying-event track을 포함하는 **일반 track 표본**이며 문제 D* daughter 표본이 아니다.

비교: 원래 full covariance / 저장하지 않는 cross term만 0으로 만든 sparse-only / 고정 schema 8 round-trip / 고정 schema 520 round-trip. schema 8은 해당 release 설정의 high-quality 경로이며 schema 520보다 세밀하지만, 이 사용 자체가 물리적으로 검증된 처방이라는 뜻은 아니다.


| Benchmark | Track N | 원본 indefinite | Sparse only | Schema 8 | Schema 520 |
|---|---|---|---|---|---|
| PR | 5644 | 0 | 703 | 3483 | 3436 |
| NPR | 6416 | 0 | 793 | 3894 | 3793 |


원래 12,060개가 모두 PSD였지만 schema 520 후 7,229개가 indefinite가 됐다. 원래 정보 손실과 원소별 압축이 실제 정상 covariance를 비정상으로 만들 수 있음을 직접 보인 것이다. Cross term 삭제만으로도 1,496개가 indefinite다. 두 효과를 단순 가산하여 원인을 몇 %씩 배정하지 않는다.

schema 8도 indefinite 개수 자체를 해소하지 못했다. 다만 normalized 최소 고유값 < -0.1인 심한 경우는 schema 520의 5,522개에서 schema 8의 290개로 줄었다. 즉 단순히 '정밀도를 높이면 모든 문제가 해결된다'는 주장도 성립하지 않는다.

이전 리포트의 full-RECO 부재는 **문제 official 후보와 동일한 이벤트**에 대한 판단이다. 이번 별도 benchmark RECO 발견으로 통제 round-trip은 가능해졌지만, 그 official 후보의 원래 covariance를 회수한 것은 아니다.

##### 같은 D* 후보의 covariance 개입

새 `CovarianceSensitivityProbe`에서 24개 frozen 대상마다 D0/D*를 처음부터 독립 refit했다. identity, `pseudoPosDefTrack()` covariance, diagonal-only, original covariance×0.5, ×2를 비교했다. 원래 track momentum/reference point와 질량 가정은 유지했다. 모두 120개 valid이고 identity slow 4-vector는 원래 replay와 정확히 일치했다. 이 sample은 deliberately stratified이므로 모집단 비율이나 평균효과 추정에 사용하지 않는다.


| 변경 | Valid N | 최대 |Δm 변화| (MeV) | 중앙 소속 변경 N |
|---|---|---|---|
| packed_posdef | 24 | 0.002156 | 0 |
| diagonal | 24 | 0.058706 | 1 |
| scale_half | 24 | 0.010664 | 0 |
| scale_double | 24 | 0.006178 | 0 |


`pseudoPosDefTrack()`는 해당 코드에서 음의 eigenvalue에 따라 diagonal을 올리는 regularization이다. [공식 구현](https://github.com/cms-sw/cmssw/blob/CMSSW_13_2_11/DataFormats/PatCandidates/src/PackedCandidate.cc#L155-L198)의 주석은 “alter values to allow for pos-def”이다. 원래 RECO covariance 복원이 아니다. 이 시험에서 작은 변화가 나온 것은 **그 특정 처리에 대한 민감도가 작은 것**이지, covariance가 물리적으로 정확하거나 어떤 가능한 covariance 오류에도 안전하다는 증거가 아니다. ×0.5/×2는 indefinite 행렬의 부호 문제를 없애지 않는 별도의 민감도 검사다.

#### 가설별 판정과 필요한 반증

| 가설 | 현재 판정 | 수행한 검증 | 남은 결정적 검증 |
|---|---|---|---|
| BDT cut이 남은 후보의 reconstruction 값을 직접 바꾼다 | 확인한 12 survivors에서 배제 | producer cut 전후 31개 값 exact equality | 큰 대표표본 일반화는 가능하나 현재 우선순위 낮음 |
| D0 topology 선택과 slow angular refit의 결합 | 직접 경로를 지지; 개별 입력 인과 미확정 | paired states, 모든 변경 순서, 조건 맞춤 | 충분한 common support와 pre-packing covariance/truth를 가진 같은 후보 비교 |
| D0 mass 폭 자체가 D* 차이의 주경로다 | 이번 중앙구간 분해에서는 작은 항 | D0 state 기여 +0.017 pp 대 angular +4.257 pp | 다른 bin/source에 대한 일반화는 별도 |
| source 혼합/weight가 주원인이다 | 단순 설명 지지되지 않음 | source-mixture 항 +0.027 pp, unit-weight 비교 | source별 generator/reco 모델 효과까지 배제하지 않음 |
| 중복 또는 원래 matching 경계 통과가 주원인이다 | 단순 설명 지지되지 않음 | multiplicity 보정, original matching 중앙 유입 0 | independent TrackingParticle association으로 잘못된 slow track 연결 확인 |
| MiniAOD packing이 covariance를 비정상으로 만든다 | 별도 실제-track 통제 실험에서 재현 | 0/12060 → 7229/12060 indefinite | 문제 후보 자체의 원본 covariance와 exact paired 비교 |
| 그 covariance 이상이 BDT-dependent peak의 주원인이다 | 입증되지 않음 | PSD도 중앙 유입; pos-def 처리 중앙 소속 0/24 변경 | 진짜 RECO covariance/track truth 사용 refit과 bias/pull 비교 |
| fitted FWHM 감소가 전체 resolution 개선이다 | 성립하지 않음 | empirical width68 증가, KDE bandwidth 민감도 | truth residual/coverage 및 shape-model uncertainty |
| 관측한 cos별 차이에 통계변동이 크게 기여한다 | 배제할 수 없음 | 10-cell 동시 구간 모두 0 포함 | 독립 MC와 사전 고정한 observable/검정 |

#### 다음 단계 및 자원 판단

추가 MC 수만 늘려도 이 sample의 통계는 개선된다. 그러나 동일한 packing을 반복한 MiniAOD만 늘리면 원래 covariance와 독립 track truth를 잃는 문제는 해결되지 않는다. 다음 생산은 두 정보를 함께 보존하도록 설계해야 한다.

현재 core contrast의 오차 약 3.34 pp를 1 pp까지 줄이려면 단순 독립 표본 scaling에서 약 11배의 유효 통계가 필요하다. 이를 raw GEN event 수와 동일시하지 않는다. topology common support 문제와 systematic/source 효과까지 해결되는 보장도 없다. 현재 점추정으로 유의도를 목표 삼아 stopping하는 설계는 권하지 않는다.

승인 요청은 `LARGE_WORK_PROPOSAL.md`에 구체화했다. 먼저 prompt/nonprompt 각각 1,000 filtered accepted events의 보존형 full-chain pilot으로 track identity/truth/packing 전후 비교 경로를 검증하고, 실제 대상-bin 수율과 자원을 측정한다. 이후 통계용 대량 production 규모는 그 결과로 다시 승인받는다. Pilot 자체로 문제-bin FWHM을 확정한다고 약속하지 않는다.

#### 코드·실행·재현성

모든 변경은 campaign study와 isolated lxplus study runtime에 한정했다. 기존 runtime 165개 파일은 이전 검증 상태와 hash가 동일하다. 후속 진단 plugin 두 개만 추가했다. 최종 소스 hash는 `followup_runtime_provenance_final.json`에 있다. canonical production 및 campaign nominal 결과는 수정하지 않았다.

새 source/control 진단 코드는 `diagnose_followup.py`, `diagnose_sources.py`; 전체 연결은 `join_diagnostics.py --all-cos`, 네 이벤트 복구 합치는 검증은 `all_cos/finalize_join.py`다. final input은 `all_cos/paired_candidates_complete.npz`이며, initial 4개 missing audit도 보존했다. `all_cos/complete_validation.json`이 최종 연결 기준이다.

주요 표: `followup_allcos/*.json,csv`, `source_diagnostics/*.json,csv`, `covariance_sensitivity_compact.json`, `packing_roundtrip_summary.json`, `packing_precision_detailed_summary.json`.
첫 baseline 재생산의 기존 1개 종료 anomaly와 setup 실패는 원래 리포트에 남아 있다. 이번 covariance 8개 batch와 packing의 PR/NPR 실행들은 모두 exit 0이다. 새 probe 첫 build의 TrackQuality 타입 오류는 생성자 호출을 수정한 뒤 build와 identity control로 검증했으며 production 결과로 사용하지 않았다.

Shapley 성분 합의 최대 오차는 중앙 gain 2.84e-14 pp, Δm 1.78e-11 MeV다. source-mixture 항등식, stage Δm, candidate join, replay identity를 assert로 검증했다. 완전한 최종 물리 uncertainty coverage 시험과는 구별한다. 그림은 PNG/PDF로 저장하고 잘림·겹침을 확인했다.



![Follow-up diagnosis](/home/jun502s/DstarAna/DStarAnalysis/Fit/PbPb/Test/SpinAlignment/output/campaign_26Sep28_official_private3M5M_source_family_1to1/study/bdt_refit_causality_20260929/followup_diagnosis.png)

![Packing round-trip](/home/jun502s/DstarAna/DStarAnalysis/Fit/PbPb/Test/SpinAlignment/output/campaign_26Sep28_official_private3M5M_source_family_1to1/study/bdt_refit_causality_20260929/packing_roundtrip.png)

Local: `/home/jun502s/DstarAna/DStarAnalysis/Fit/PbPb/Test/SpinAlignment/output/campaign_26Sep28_official_private3M5M_source_family_1to1/study/bdt_refit_causality_20260929`

Lxplus: `/eos/home-j/junseok/analysis/dstarana/VertexCompositeStudies/studies/refit_diagnostics_20260918/bdt_refit_causality_20260929`


<!-- EXISTING_RECO_20260929 -->

### Preserved RECO: exact original/packed comparison (2026-09-29)

#### Confirmed results

Reprocessed the retained `Prompt_100_11/step3.root` and `Nonprompt_100_21/step3.root` (100 events each). No new generation or detector simulation was needed. PAT was run in the original CMSSW_13_2_16_patch1 release, adding output retention of original tracks, PF candidates, and EDM associations. D0/D* production and the diagnostic plugin ran in the isolated study CMSSW_13_2_11 runtime.

| Quantity | Prompt | Nonprompt |
|---|---:|---:|
| Input events audited | 100 | 100 |
| Reconstructed D* candidates, before GEN matching | 280 | 312 |
| Candidates geometrically matched to the correct K/pi/slow GEN decay assignment | 8 | 6 |
| Events containing those candidates | 7 | 6 |
| Candidates with exact original-track identities for all three tracks | 8 | 6 |
| Matched candidates at reco pT 7–10 GeV/c | 1 | 2 |
| Also with abs(y)<0.3 | 0 | 1 |
| Also with centrality 0–10%, before cosine/DCA cuts | **0** | **0** |

Therefore the original target (pT 7–10, abs(y)<0.3, centrality 0–10%, tracker-axis abs(cos theta*) 0.6–0.8, DCA 0–0.08 cm) has no candidate in this sample. These results cannot measure its FWHM or establish its causal origin.

Exact original/packed track identity comes from EDM product IDs and association keys, **not** delta-R. GEN matching is separately a geometric selection (same charge, delta-R<0.03 for all three daughters with the assigned K/pi mass hypothesis); it is not independent hit-based truth. Electron/GSF packed tracks without the appropriate original GSF input are explicitly left unmatched, not substituted by generalTracks.

Each selected nominal candidate is frozen for all comparisons, including its BDT score, kinematics, and track identities. The analyzer records all input events on a separate path, including those rejected by the nominal analysis path; its broad candidate counts should not be interpreted as the final analysis yield.

#### Controlled refits and closure

Five modes were evaluated with fresh D0 and D* fits: packed baseline, reconstruction of the identical reco::Track, original slow-pion covariance only, original covariance for all three tracks, and all three complete original RECO tracks. No PSD regularization or adjusted boundary was applied.

All **70/70** refits (14 candidates times five modes) produced valid D0/D* trees. The identical-track control changed the stored slow-pion state by exactly zero. The independent packed replay differed from the production stored slow-pion state by at most 9.55e-17 over the stored components. All four cmsRun FrameworkJobReports (two PAT, two analysis) have no FrameworkError and all 200 input events appear in the audit output.

Shift below is `(M(D*)-M(D0))_alternative - (M(D*)-M(D0))_packed`, using the fitted D0 child in the final D* tree, in MeV.

| Intervention | PR shift range | NPR shift range |
|---|---:|---:|
| Identical-track control | 0 | 0 |
| Original slow-pion covariance only | −0.04217 to +0.13332 | −0.25493 to +0.03838 |
| Original covariance of all three tracks | −0.03611 to +0.13484 | −0.25500 to +0.03744 |
| All three original tracks, including their state | −0.03778 to +0.13506 | −0.25889 to +0.04482 |

The original covariance is expressed in the same five track-parameter coordinates. Covariance-only replacement holds the packed reference state fixed; complete-original-track replacement additionally changes the compressed reference state/momentum. These are distinct interventions and are both retained.

These 14 are broad diagnostic candidates before final mass/cosine/DCA selections. The largest slow-covariance shift is the NPR candidate `(run,lumi,event,index)=(21,1,937,0)`, pT=5.70373 GeV/c, y=0.29359, centrality bin 49 (24.5–25%), BDT=0.98082: fitted delta mass 145.89841 → 145.64348 MeV. It is outside the target pT/centrality selection. The PR maximum quoted in the table has packed fitted delta mass 157.81716 MeV, outside the earlier 140–153 MeV study window. Per-candidate coordinates and shifts are preserved in `existing_reco/paired_shifts.csv`.

The original covariance matrices of all 42 selected track instances are positive definite. Among the 14 packed slow-pion instances, **8 are indefinite** (PR4, NPR4); another three packed D0-daughter instances are indefinite. Counts are candidate-track instances, not unique tracks. This extends the earlier generic-track packing experiment to actual selected D* candidates.

**Confirmed:** original-covariance replacement changes the fitted delta mass for this small, broad sample, with a maximum absolute slow-covariance-only shift of 0.25493 MeV. The earlier 0.00216 MeV bound from `pseudoPosDefTrack()` on a different 24-candidate sample was a regularization sensitivity test; it was not a bound on restoring the true original covariance.

**Not established:** that covariance packing explains the prompt/nonprompt FWHM difference in the target bin, its population-average effect, the responsible BDT input, or uncertainty coverage. Positive definiteness alone does not validate a covariance or a resulting uncertainty. No final physics production treatment follows from these 14 candidates.

#### Official implementation evidence

The [CMSSW PATPackedCandidateProducer, lines 397–416](https://github.com/cms-sw/cmssw/blob/CMSSW_13_2_16_patch1/PhysicsTools/PatAlgos/plugins/PATPackedCandidateProducer.cc#L397-L416) explicitly provides the track association: “include also the mapping track -> packed PFCand”. The retained association is inverted by exact product/key identity in the study.

The [CMSSW PATLostTracks, lines 220–244](https://github.com/cms-sw/cmssw/blob/CMSSW_13_2_16_patch1/PhysicsTools/PatAlgos/plugins/PATLostTracks.cc#L220-L244) constructs the corresponding lost-track association and notes “not done for the lostTrack:eleTracks collection”. Unsupported identities are reported rather than guessed.

#### Additional work decision

The existing sample validates the comparison method but has zero target-bin candidates. Additional production is needed to study the target distribution. A bounded broad-acceptance pilot was submitted to **CERN HTCondor cluster 12717346**: PR1000 + NPR1000 accepted stored events planned, split into 20 independent 100-event jobs with distinct run IDs and random seeds. The same generator, decay, pT/rapidity-filter and HYDJET settings as the benchmark are retained. Each job keeps RECO, associated MiniAOD, analysis ROOT, paired records, logs, configs and event-identity checks.

This pilot should increase broad-sample diagnostics; it **does not guarantee adequate target-bin FWHM precision**. No quantitative target yield is inferred from zero observations. The reused 325-event HYDJET input also limits background independence. No automatic production beyond this pilot is authorized by this study plan.

User authorized conditional parallel submission after the existing-data check. CERN HTCondor is used for this existing lxplus production task: the KU submit host and checked worker lack CMSSW CVMFS, while the CERN inputs/runtime are available. The additional site-permission question was redundant with this authorization and was not treated as a new approval gate. First queue check at 2026-09-28 23:05:12 UTC: **20 idle, zero held**. Submission is confirmed; generated physics results are not yet available. `parallel_pilot_submission.json` records submission/log/output paths and immutable worker/bundle hashes.

The 20-job dry run passed. Configurations for all four simulation/PAT stages parse in their correct releases. The packaged, relocated CMSSW runtime reprocessed existing PR100 and reproduced its 108 JSONL records byte for byte (100 events + 8 candidates). The event-identity validator accepted the preserved baseline fixtures and rejected a deliberately mismatched event ID at unchanged event count; fixtures are not mislabeled as newly generated pilot output. CERN native delegated Kerberos credentials were confirmed valid and an actual checksum-verified EOS upload succeeded. A separate GSI-only EOS test failed; the pilot relies on CERN's native credential delegation for EOS, not that failed test.

#### Reproducibility and failures

Local scripts: `OriginalCovarianceProbe.cc`, `prepare_existing_reco_remote.py`, `run_existing_analysis_remote.py`. Local evidence: `existing_reco/summary.json`, PR/NPR `pairs.jsonl`, and four FrameworkJobReports. Remote configs, logs and ROOT outputs are under `bdt_refit_causality_20260929/existing_reco/` on EOS.

The first PAT launch referenced a benchmark runtime that had been removed. No ROOT was produced despite its wrapper returning zero; this is explicitly not a successful run. A study-local runtime was restored, and output/FJR checks were added. The first plugin build rejected a missing std::array initializer brace; the corrected build passed. Canonical production and retained benchmark inputs were not modified.

최종 제출 후 확인: cluster `12717346`의 **20/20 작업 Running, Held 0**. 실행 중이며 산출물/physics QA 완료를 뜻하지 않는다.

<!-- PILOT_COVARIANCE_20260929 -->
#### Completed 2,000-event pilot: covariance audit

All 20 jobs completed: PR1,000 + NPR1,000 stored events, 54 PR + 54 NPR geometrically GEN-matched D* candidates, all with exact original/packed daughter identities. Original covariance matrices are positive definite for all 324 track instances. Packed slow-pion matrices are indefinite in 60/108 cases (PR34/54, NPR26/54), versus 10/216 D0 daughter instances (10/214 unique tracks).

Restoring only the original slow-pion covariance, while preserving packed states and both D0 daughters, changes delta m by >0.1 MeV in 24/108 candidates; the signed range is -1.34278 to +0.64296 MeV. Identity-control shifts are at most 1.07e-11 MeV. All 540 mode/candidate fits are valid, showing that fit validity alone does not detect this input-covariance defect.

This directly confirms a packed-covariance problem and its refit impact. It does not establish the cause or size of the particular target-bin BDT-dependent FWHM difference: the pilot has zero candidates after target pT/y/centrality cuts. The claim concerns packed-track covariance used for this refit, not all MiniAOD uses. No further production or canonical-code changes were made. Full methods, numerical example, sources and remaining hypothesis: `PILOT_COVARIANCE_REPORT.md`. Exact results: `pilot_covariance_summary.json`, `pilot_covariance_candidates.csv`.

<!-- OFFICIAL_POSDEF_108_20260929 -->
#### Official pseudoPosDefTrack treatment: completed comparison

Study-only unpacker implemented; CERN cluster12747673 completed20/20 jobs, reusing107 events containing108 baseline candidates. Original production unpacker and D0Fitter remain unchanged. Official covariance correction removes slow-pion PSD violations60->0/108 and D0-daughter violations10->0/216. Full reconstruction retains all108 exact candidate identities; delta-m changes by>0.1MeV in3 candidates (maximum0.674625MeV). D0-first mass changes by>0.1MeV in2 candidates (maximum0.855496MeV), vertex displacement reaches9.07039um, and9 BDT scores change (maximum0.0193592); no candidate crossesBDT0.95.

This repairs the observed non-PSD matrices but is not original-RECO recovery:25/108 corrected delta-m values still differ by>0.1MeV from the original-track refit, versus27/108 before. These25 are reference differences, not failed fits. No target-bin FWHM conclusion follows. Details, proposed production diff and complete per-candidate results: `official_posdef_108/REPORT.md`, `official_posdef_108/summary.json`, `official_posdef_108/candidates.csv`.


<!-- REFIT_BEFORE_AFTER_20260929 -->
#### Each input representation before versus after the D* fit

Original RECO tracks do not suppress the refit-induced changes in the same108 baseline candidates: median absolute delta-m change packed0.170888 vs RECO0.179082MeV; changes>0.1MeV66 vs71. Median absolute slow-pion delta_eta0.000906726 vs0.000879767; delta_phi0.00318539 vs0.00310496rad. This is each representation compared to its own pre-D* state, not the difference between two final fits. Track transport and vertex constraints are included in these changes. The angular-change/conditional-peak observation does not by itself establish a covariance error as its sole cause. Definitions, matched example and exact table: `official_posdef_108/REFIT_BEFORE_AFTER.md`.


#### Actual target-bin posdef replay — 2026-09-29

CERN Condor cluster12755181 submitted: 64 jobs, max16 materialized; original official pT4 target candidates PR716+NPR565 in1,258 events/877MiniAOD files. Baseline and posdef reconstruct the same events. One-event baseline reproduction and corrected execution passed (21.11s/24.23s). Full results pending. See target_bin_posdef/REPORT.md and submission.json. No new MC generation or canonical production edits.


#### Matched shape / BDT event bootstrap — 2026-09-29

Completed matched_shape_bootstrap/REPORT.md: requested official pT4 sample linked1,281/1,281; common-window1,280candidates/1,257events; 400 event-Poisson replicas and12nominal DSCB fits. Prompt tight−inclusive after-refit FWHM−0.201±0.239MeV, empirical width68+0.003±0.059MeV. Pass−fail refit interaction FWHM−0.613±0.339MeV, width68−0.026±0.155MeV, central fraction+5.185±3.279pp; all95% percentile intervals include0. DSCB sigma floor touched in195/400prompt-tight-after replicas. General core/selection hint remains, but the requested robustness gate is not met; no extra MC generation, BDT training or yield-closure claim. Both plots and numeric results saved.
<!-- source:SRC-12:end -->

<a id="src-13"></a>

## SRC-13 — bdt_refit_causality_20260929/STATUS.md

**기록 구분:** 해당 시점 상태·제출 기록 (현재 queue 조회 아님). **원문:** [bdt_refit_causality_20260929/STATUS.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/STATUS.md). **SHA256:** `dc24cdde53e1616a6344cf66a4d52f25321c9f15232f0e6a28add8ecc36067de`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-13:start -->
### 상태 — 2026-09-29

<!-- embedded-truth-latest:start -->
**최신 생성점 복원 진단: [embedded_truth/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/embedded_truth/REPORT.md).**

- 기존 MiniAOD의 배경 GEN ancestry와 실제 embedding 소스로 signal 생성점을 복원하고, 같은 기준점에서 입력·fitted slow pion을 비교했다. 독립적으로 저장된 simulation vertex와의 직접 대조는 아니며, reco PV를 truth로 대체하지 않았다.
- 검증된 후보 1229개, 작업 61/63개 완료. 미수락 작업: NPR_15, NPR_24. Prompt 716개는 모두 포함. 이전 단계에서 누락된 NPR_26의 17개는 별도이며 이번 실행에도 포함되지 않는다. 상세 실행 상태와 원격 입력 오류는 records.json 및 REPORT.md에 기록한다.
- Prompt BDT 통과 491개: 같은 생성점 기준 평균 방향 오차 ΔR = 0.005580 → 0.004837. Fitted vertex 위치 오차 중앙값 1.146 mm, GEN 비행 방향에 수직인 성분 중앙값 24.1 μm.
- 같은 fitted state의 운동량 평가점을 바꾸면 중앙-bin 비율 19.791% → 13.405%. 원래 질량 잔차 width68 = 1.3045 → 1.4486 MeV; 공통 생성점에서는 1.3044 → 1.3702 MeV. 좁은 fitted core를 전체 질량 정확도 개선으로 볼 수 없다.
- 해석: fitted vertex의 위치에 따른 상대각 변화가 질량 모양에 크게 작용한다. 이는 전체 reconstruction의 오류/정당성, BDT의 유일한 원인, covariance coverage 또는 yield closure의 확정과는 다르다. GEN 기준점은 DATA용 처치가 아니다. Production 소스 해시 유지.

아래 블록은 이전 시점의 기록이다. 생성점 정보를 전혀 이용할 수 없다는 이전 제한은 이번 source-derived 복원으로 진전됐으며, 독립 simulation-vertex 대조의 한계는 남는다.
<!-- embedded-truth-latest:end -->

<!-- geometry-selection-latest:start -->
**Latest signed-geometry follow-up:** [geometry_selection/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/geometry_selection/REPORT.md). Pre-fit input-line geometry predicts the propagation Delta-m sign for96.5% of weighted prompt BDT-pass candidates (95.1%fail); slow-pion phi-only rotation reproduces the full propagation mass shift within6.32e-6MeV. Nearly parallel D0/slow-pion trajectories give a weak longitudinal common-vertex constraint. Prompt pass has50central-bin entrants and33exits during propagation, but only46.8%move toward the center; core gain is not overall resolution improvement. No unique BDT-topology cause or significant BDT-specific interaction is established. Actual SIM provenance and one MiniAOD event support unsmeared signal GEN stored alongside translated background GEN; exact per-event embedding-point validation remains unresolved, so no angular-truth or yield-closure claim. All1264available candidates, five plots, matching diagnostics and validation are saved; production unchanged.
<!-- geometry-selection-latest:end -->


<!-- truth-geometry-latest:start -->
**최신 진단: [truth_geometry/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/truth_geometry/REPORT.md).**

- 공통 기준점 진단63/64작업,1,264/1,281후보 검증. Prompt716개 전부 포함. Nonprompt17개는 FNAL 원격 파일 읽기 시간 초과로 제외; 마지막 작업은 실행 중이 아니라 실패 후 held 상태.
- Prompt BDT 통과491개: 중앙-bin15.259%→18.761%→19.791%(입력→결정된 D*vertex로 전파→최종 상태, 내부 질량차 정의). 기준점은 전체 fit의 결과이므로 독립적인 인과 분해로 단정하지 않음.
- GEN 질량차 잔차 width68은1.3045→1.4497MeV. Posdef 대조군에서도1.4497→1.4502MeV로 거의 유지.
- 입력과 출력의 상관을 무시한 pull은 사용하지 않음. Prompt GEN 원점과 detector 좌표 연결이 확인되지 않아 GEN 위치를 기준으로 한 각도오차 개선 주장은 보류. 확인한 MiniAOD 이벤트의 hit 기반 GEN association 두 제품은 크기0.
- Posdef campaign은62/64작업 성공,2개 입력 접근 실패. 새 MC생성·BDT재훈련·추가bootstrap·production소스변경 없음. 남은 입력경로·가설·검증방법 및 실제실행 오류는 상세리포트에 보존.
<!-- truth-geometry-latest:end -->

**최신: 공식 pseudoPosDefTrack 처치 study 완료. 20/20 작업·108개 후보 전후 비교 완료. Covariance 이상 slow60→0, D0 daughter10→0. BDT9개 변화, 0.95 통과 변경0. 원본 RECO와 Δm 차이>0.1MeV는25개 남음. Production 원본 수정 없음. 상세: official_posdef_108/REPORT.md.**


**최신: 20/20 pilot 완료, 2,000이벤트·D* 108개 covariance audit 완료. Slow-pion packed covariance 60/108 indefinite; 원본 covariance만 복원 시 24/108에서 |Δm 변화|>0.1 MeV. 상세: PILOT_COVARIANCE_REPORT.md. 특정 target-bin FWHM 원인은 아직 미확정.**

아래는 이전 단계의 시점별 기록이다.

**보존 RECO 200이벤트 검사 완료. CERN HTCondor cluster `12717346`에 20개 병렬 pilot 작업 제출 완료.**

완료: 전체 cos 6,159개 연결, 원본 MiniAOD 누락 4개 복구, paired decomposition, 운동학/topology 조정, source/weight/duplicate/matching/width 진단, 24개 covariance 개입 재생산, 20개 보존 RECO event의 12,060-track packing round-trip.

확인: slow-pion angular refit이 관측 중앙집중의 주된 운동학적 경로. 정상 covariance를 indefinite로 만드는 packing 효과는 실제 RECO track 통제 실험에서 재현.

미확정: packing 이상이 특정 BDT/cos peak의 주원인인지, 어떤 BDT 입력이 인과적으로 작용하는지. 전체 cos 비교에서 통계변동 배제 불가. 기존 candidate의 원본 covariance/truth가 필요한 상태.

최신 상세 결과: `FOLLOWUP_REPORT.md` 및 이어 쓴 `REPORT.md`.
준석 지시: 먼저 기존 RECO를 확인하고, 필요하면 Condor로 병렬 제출한다. 이전 신규 pilot 승인 대기는 이 조건부 지시로 대체됐다. 구체적인 추가 작업량은 기존 표본의 후보 수/검증 결과로 정한다.

기존 표본 결과: PR100 + NPR100이벤트를 PAT 재실행하고 실제 D* 후보를 검사했다. 넓은 범위의 기하학적 GEN-matched 후보는 PR8 + NPR6이며, 14개 모두 세 track의 원본/packed identity가 정확히 연결됐다. 70/70 refit valid, 동일-covariance control 차이 0. 목표 pT7–10, abs(y)<0.3, centrality0–10% 후보는 cosine/DCA 적용 전부터 0개다. 넓은 표본의 slow covariance-only 교체는 최대 abs(delta-m shift)=0.25493 MeV였으나 목표 bin 원인 판정은 불가능하다.

추가 pilot: PR1000 + NPR1000 stored events 생성 예정, 100이벤트/작업, 1코어/작업, 총20개. CERN HTCondor cluster12717346. 첫 queue 확인(2026-09-28 23:05:12 UTC): **대기20, 보류0**. 제출은 완료됐지만 생성/분석 결과는 아직 없다. 2000이벤트로 목표-bin FWHM 정밀도가 확보된다는 보장은 없고, 자동 확대하지 않는다.

제출 파일: `/afs/cern.ch/user/j/junseok/private/bdt_refit_parallel_20260929/submit.sub`
로그/텍스트 결과: 같은 폴더 `jobs/{PR,NPR}_*/`
ROOT 출력: 원격 study의 `parallel_pilot/{PR,NPR}_*/`
정확한 경로/해시/queue 기록: `parallel_pilot_submission.json`, `parallel_pilot_queue.json`.

검증: 20-job dry-run 통과, GEN–PAT 4단계 설정 parse 통과, 이동된 runtime의 PR100 재분석 기록이 기존 결과와 바이트 단위로 일치. 같은 이벤트 수의 잘못된 event identity는 검증기가 거부함. CERN native delegated Kerberos가 유효하며 실제 EOS 쓰기 확인. GSI-only EOS 인증은 실패하여 worker 인증으로 간주하지 않음. 임시 runtime 이동 검증의 ROOT/추출본/테스트 wrapper는 검증 후 정리하고 진단 receipt/log/FJR는 보존.

최신 상세: `EXISTING_RECO_REPORT.md` 및 `REPORT.md`의 마지막 절. Canonical production과 기존 benchmark 입력은 변경하지 않았다.

최종 제출 후 확인: cluster `12717346`의 **20/20 작업 Running, Held 0**. 실행 중이며 산출물/physics QA 완료를 뜻하지 않는다.


#### Pilot progress — 2026-09-29T01:26:16.126396+00:00

15/20 jobs completed with exit 0, completed markers and all three ROOT outputs present. PR: 1000 events, 54 geometrically GEN-matched candidates with exact original/packed identity. NPR: 500 events, 24 such candidates. Remaining 5 NPR jobs running; no held jobs. Target pT7–10, abs(y)<0.3, centrality0–10%: 0 candidates before cosine/DCA cuts. This partial sample cannot establish the target-bin FWHM cause. Details: `parallel_pilot_progress.json`.


#### Actual target-bin posdef replay — 2026-09-29

CERN Condor cluster12755181 submitted: 64 jobs, max16 materialized; original official pT4 target candidates PR716+NPR565 in1,258 events/877MiniAOD files. Baseline and posdef reconstruct the same events. One-event baseline reproduction and corrected execution passed (21.11s/24.23s). Full results pending. See target_bin_posdef/REPORT.md and submission.json. No new MC generation or canonical production edits.


#### Matched shape / BDT event bootstrap — 2026-09-29

Completed matched_shape_bootstrap/REPORT.md: requested official pT4 sample linked1,281/1,281; common-window1,280candidates/1,257events; 400 event-Poisson replicas and12nominal DSCB fits. Prompt tight−inclusive after-refit FWHM−0.201±0.239MeV, empirical width68+0.003±0.059MeV. Pass−fail refit interaction FWHM−0.613±0.339MeV, width68−0.026±0.155MeV, central fraction+5.185±3.279pp; all95% percentile intervals include0. DSCB sigma floor touched in195/400prompt-tight-after replicas. General core/selection hint remains, but the requested robustness gate is not met; no extra MC generation, BDT training or yield-closure claim. Both plots and numeric results saved.
<!-- source:SRC-13:end -->

<a id="src-14"></a>

## SRC-14 — bdt_refit_causality_20260929/bdt_input_attribution/PLAN.md

**기록 구분:** 계획·제안 원문 (완료의 증거 아님). **원문:** [bdt_refit_causality_20260929/bdt_input_attribution/PLAN.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/bdt_input_attribution/PLAN.md). **SHA256:** `777833742b64ed9f60e3b322979bc05a3c999f17736a9c875d0cd60a1ce78e94`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-14:start -->
### Exact BDT selection and refit response

Goal: test whether actual deployed BDT inputs identify the selected candidate population causing the observed after-refit core response. Production source/model/training and candidate weights remain unchanged.

Done: establish exact model bytes and input order; reproduce saved candidate scores before attribution; connect input groups to signed mass/core migration using the same candidates and event resampling; report explained versus residual selection differences with overlap limitations and plots. Do not label SHAP or observational balancing as physical causal attribution. No new MC generation, model retraining or automatic production changes.

1. Inspect preserved ntuple branches and D0Fitter input definitions. Check candidate identity, float conversion, positive/negative daughter order and exact 24 replay vectors. If ntuple inputs reproduce the model score/threshold decisions, analyze all recovered candidates; otherwise diagnose missing information and use a bounded study replay, not guessed inputs.
2. Verify deployed ONNX against native XGBoost/tree representation with the full ensemble. Record hashes, prediction errors and BDT0.95 migrations.
3. Compute grouped exact model contributions (pointing; displacement/significance; vertex/track DCA; daughter kinematics; daughter errors; D0 kinematics/centrality). Link groups to central entrants/exits and signed refit response. Preserve correlated variables in groups; no independent feature shuffling as physical evidence.
4. Compare pass/fail responses on measured common support, assess feature balance and event-bootstrap uncertainty. Compare FWHM, width68 and fixed-bin changes; include nominal reproduction controls.
5. Validate numeric identities and likelihoods, render/inspect plots, write REPORT.md and update parent status. Run local analysis and necessary remote extraction in foreground; request approval only if a genuinely large new replay is needed.

Files: all new scripts, immutable copied model/source evidence, extraction records, CSV/NPZ, ROOT-interpreted shape-fit macro, plots and report under this directory. Commands: python3/uproot extraction; existing d0_training_py310 environment for ONNX/XGBoost; interpreted ROOT using thisroot.sh; ssh lx read/extract existing outputs. No ROOT macro compilation.

Completed: full candidate score reproduction; grouped model explanations; all matching calipers and balance checks; 200 nominal shapes; 2000 event-bootstrap metrics and 300 FWHM replicas with explicit remaining fit-quality flags; plots and report. Unique causal attribution remains unsupported by common-support/statistical precision, as documented. No production changes.
<!-- source:SRC-14:end -->

<a id="src-15"></a>

## SRC-15 — bdt_refit_causality_20260929/bdt_input_attribution/REPORT.md

**기록 구분:** 조사 보고서·해석 (표본·시점별 결과). **원문:** [bdt_refit_causality_20260929/bdt_input_attribution/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/bdt_input_attribution/REPORT.md). **SHA256:** `bb799b50de530b9b7f2987015df9fa0890cda5915f54916a92bbab9c3ede9bcd`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-15:start -->
### 실제 BDT 입력과 D* refit 반응 — 2026-09-29

#### 물리적 결론

**BDT 통과·탈락을 나누는 실제 입력은 주로 D0 pointing과 비행거리 유의도다. Pointing을 비슷하게 맞춘 후보에서는 통과 집단만 더 강하게 좁아지는 명목 FWHM 차이가 크게 줄었다. 이는 D0 기하 선택과 refit 반응의 결합이라는 해석을 지지한다. 그러나 전체 현상을 특정 입력 하나 또는 입력 조합의 인과효과로 확정하지는 못했다.**

- **확인:** 전체 대상 1,281개에서 복원한 20입력으로 실제 배포 ONNX의 저장 점수를 정확히 재현했다. BDT0.95 판정 변경 0개다. 모델 이름으로 추정한 중요도가 아니라 실제 후보와 실제 배포 모델을 사용했다.
- **확인:** Prompt의 통과−탈락 평균 logit 차이 4.705 중 pointing 기여 2.061, 비행거리·유의도 기여 1.846이다. 합계 약83%는 **점수 차이의 분해**이며 FWHM 변화의 원인 비율이 아니다. Daughter 운동학 기여 차이는 0.035에 그쳤다.
- **확인:** Prompt에서 refit FWHM 변화의 통과−탈락 차이는 원래 −0.613MeV, pointing을 맞춘127쌍에서는 −0.032MeV, daughter 운동학을 맞춘218쌍에서는 −0.463MeV다.
- **해석:** 현재 결과는 단순 daughter pT/eta 구성 차이보다 **D0 비행선 정렬을 선택하는 효과**를 우선 조사할 근거다. Pointing을 맞추면 탈락 집단도 refit 후 더 좁아진다. 즉 통과 집단만 원래 특별한 mass resolution을 가진다는 설명보다, 선택된 기하에 따라 refit 반응이 달라진다는 설명과 부합한다.
- **한계:** 127쌍은 원래 통과 집단의 가중합 약26%만 대표한다. 전체 입력 또는 pointing·비행거리·vertex 품질을 동시에 충분히 맞출 공통 표본이 부족하다. FWHM 및 중앙-bin 차이의95% bootstrap 구간은0을 포함한다. 따라서 원인 분리의 완결, reconstruction의 옳고 그름, DATA 재처리 필요성을 주장할 수 없다.

BDT가 후보의 값을 바꾸는 것은 아니다. `Δm_after = Δm_before + R(기하, 운동량, covariance)`에서 BDT가 선택하는 기하에 따라 refit 반응 R의 분포가 달라질 수 있다. **Refit 전 질량 분포가 비슷하다는 것은 refit 반응과 질량의 결합분포까지 같다는 뜻이 아니다.** 이전 실제 fitter 입력 개입에서 확인한 기하→방향 변화→질량 반응과 이번 점수·matching 결과를 합쳐 해석해야 한다.

#### 표본과 비교 정의

Campaign: `campaign_26Sep28_official_private3M5M_source_family_1to1`의 study. 이번 표본은 **official PR/NPR pThat2/pT4**이며 private MC를 섞지 않았다.

- D* 7≤pT<10GeV/c, |y|<0.3, centrality0–10%, tracker축 0.6≤|cosθ*|<0.8, DCA0–0.08cm.
- 기존 동일 후보 자료 `../all_cos/paired_candidates_complete.npz`, 대상 manifest `../target_bin_posdef/jobs.json`.
- 입력 재현 대상1,281개: PR716/NPR565. 기존 전후 공통140≤Δm<153MeV window 적용 시1,280개,1,257event: PR715/NPR565. 제외된PR1개는 이번 결과에 맞춘 새로운 cut이 아니다.
- 통과 BDT≥0.95, 탈락0<BDT<0.95. 전후 비교에는 같은 후보·같은 원래 가중치를 사용한다.
- Matched 비교는 반대 BDT 집단의 후보를 새로 짝지은 부분 표본이다. 각 쌍에는 두 원래 가중치의 기하평균을 공통 적용한다. 원래 전체 표본의 가중 추정량과 같다고 취급하지 않는다.

|집단|N|sumw|sumw2|N_eff|
|---|---:|---:|---:|---:|
|PR 전체|715|6875.728845|66602.036650|709.823|
|PR 통과|491|4704.731102|45560.825934|485.823|
|PR 탈락|224|2170.997743|21041.210716|224.000|
|NPR 전체|565|447.802642|361.008154|555.464|
|NPR 통과|388|307.749472|248.074678|381.779|
|NPR 탈락|177|140.053169|112.933476|173.685|

N_eff=(sumw)^2/sumw2는 후보 수가 아니다. 이번 bootstrap은 고정된 기존 가중치에 조건부이며 source balance weight 추정의 불확실성을 추가하지 않았다.

#### 1. 실제 inference 입력 검증

`D0Fitter_evidence.cc`, `PAT6_evidence.cc`는 보존된 CMSSW13_2_11 study runtime에서 읽은 소스 복사본이다. 실제 모델은 `deployed.onnx`, matching native model은 `native.json`에 보존했다.

- ONNX 547090bytes, SHA256 `cd431de656a401610062481e5b6a88f20b534231836f631e9cf49d2461bbc024`.
- 전체561trees 사용. `best_iteration=510`의511tree prefix를 사용하지 않았다.
- 16,937개 node의 구조, feature, threshold, missing 방향, leaf weight 일치. Base margin/logistic도 확인했다.
- 1,281개 저장 ONNX 점수와 재계산 점수의 최대 차이 **0.0**. Native/ONNX 차이 최대1.073e−6, 판정은 동일하다.
- 정확한 inference vector가 별도로 기록된24개와도 대조했다. Ntuple float 저장/복원 차이 최대4.768e−7이다. **20입력 모두 bit-identical하다고 주장하지 않으며 점수 재현은 전수 정확하다.**
- `Trk3DDCA`는 D0 daughter 두 track의 DCA다. `pTerrD1/2`는 상대오차가 아닌 원래 daughter track의 절대 ptError다. Daughter 순서는 positive/negative이며 K/pi 순서로 대체하지 않았다.
- NPR index8051은 저장된 비행거리 유의도가NaN이다. D0Fitter684–685행의 `sigma > 0 ? length/sigma : 0`를 그대로 재현해 inference 입력은0으로 만들었다. 이 단계를 빠뜨리면 저장점수0.9994699 대신0.9885978이 나온다. 이는 **이미 존재하는 runtime 정의를 복원한 것**이며 이번에 production의 NaN 처리를 바꾼 것이 아니다.
- PR index685의2D pointing NaN은 그대로 모델에 전달했다. 임의0치환을 하지 않았다.

보존된 baseline64작업 중 `NPR_24`, `NPR_26`은 FJR에FileOpenError가 있어 완료 자료로 수락하지 않았다. 이34후보는 GP에 보존된 원래 diagnostic ROOT에서 원본LFN+run/lumi/event64+K/pi/slow track key를 정확히 연결했다. 저장 MVA·nominal Δm도 검증했다. 다른 source/후보로 대체하지 않았다. 파일·entry·후보·해시 기록은 `extracted.json`에 있다.

#### 2. 어느 입력이 실제 높은 점수를 만드는가

XGBoost의 exact TreeSHAP를 전체561tree에 적용했다. 기여의 합+bias가 raw margin과 최대1.097e−5 이내로 일치한다. 그룹 내 상관 변수는 함께 합쳤다.

|입력군|PR 통과−탈락 기여(logit)|NPR 통과−탈락 기여(logit)|
|---|---:|---:|
|D0 pointing: 3D/2D angle 및 cos|2.061|1.940|
|비행거리·유의도: 3D/2D|1.846|1.012|
|Vertex probability 및 D0 track DCA|0.449|0.320|
|Daughter pT/eta/Δeta|0.035|0.200|
|Daughter 절대 ptError|0.044|0.083|
|D0 pT/y 및 centrality|0.270|0.253|

PR의 개별 변수 중 큰 항은3D decay-length significance(+1.356),3D pointing angle(+1.270),2D significance(+0.786),3D cos-pointing(+0.565),track DCA(+0.403)다. 상관 변수들 사이에 TreeSHAP 분해가 나뉘므로 이를 독립적인 물리효과의 순위로 바꾸면 안 된다.

고정 중앙-bin[145.2881355932,145.5084745763)MeV로 들어온 후보와 빠져나간 후보를 비교하면 PR pointing 점수 기여 차이는+0.530logit,95%구간[+0.031,+1.071]이다. 여러 입력군을 비교한 **보정하지 않은 구간**이며 이것 하나를 발견 유의도로 해석하지 않는다. 전후 중앙-bin 자체의 통과−탈락 반응 차이는+5.185pp이고 구간은0을 포함한다.

#### 3. 입력을 비슷하게 맞췄을 때의 질량 반응

Matching에는 질량이나 refit 이동량을 사용하지 않았다. 각 입력의 가중 percentile rank 거리로 통과·탈락 후보를1:1, 중복 없이 대응했다. RMS caliper0.10,0.05,0.025,0.01과 각 축2배caliper를 모두 시험했다. 최소30쌍이고 raw/rank의 모든 absolute standardized mean difference(SMD)가0.10이하일 때만 균형 검증 통과로 표시했다. 모든 시도는 `matching.csv`, 변수별균형은 `balance.csv`, 실제쌍은 `matched_pairs.csv`에 남겼다.

대표 PR 비교는 mass 결과를 보기 전에 정한 기준으로 **가장 넓은 caliper 중 균형 검증을 통과한 것**이다: pointing0.05, daughter kinematics0.10.

|PR 집단|통과 전→후 FWHM(MeV)|탈락 전→후 FWHM(MeV)|통과−탈락 refit 변화 차이(MeV)|95% event-bootstrap 구간|
|---|---:|---:|---:|---:|
|원래 표본491/224개|1.275→0.610|1.191→1.140|−0.613|[−0.934,+0.542]|
|Pointing matching127쌍|1.069→0.677|1.174→0.815|−0.032|[−0.861,+0.734]|
|Daughter 운동학 matching218쌍|1.254→0.663|1.177→1.050|−0.463|[−0.744,+0.596]|

차이는 `(FWHM_after−FWHM_before)_pass − (FWHM_after−FWHM_before)_fail`다. 음수이면 통과 집단에서 더 많이 좁아졌다. Pointing matching이 before 분포까지 완전히 같게 만드는 것은 아니므로 after 폭만 비교하지 않았다.

- Pointing127쌍: max rawSMD0.0681, rankSMD0.0340. 원래 통과 가중합26.0%,탈락56.7%만 포함한다. 중앙-bin refit 증가의 통과−탈락 차이는−1.584pp[−11.577,+8.581].
- Daughter218쌍: max rawSMD0.0691, rankSMD0.0659. 통과44.7%,탈락97.3% 포함. 중앙-bin 차이+4.020pp[−3.491,+12.090].
- Pointing+daughter 운동학을 함께 맞추면 느슨한caliper에서63쌍이지만 SMD검증 실패; 더 좁은0.05에서는2쌍뿐이다.
- Pointing+비행거리+vertex/DCA를 함께 맞춘 topology는 느슨한caliper65쌍이지만 SMD검증 실패;0.05에서는8쌍이다.
- 전체20입력을 동시에 지정 caliper 안에서 맞춘 쌍은 PR/NPR 모두0개다. **이는 이번 표본·matching 기준에서의 공통 영역 부족이지, 물리적으로 대응 후보가 절대 존재하지 않는다는 증명은 아니다.** BDT 통과/탈락 자체가 같은20입력의 결정함수라는 점도 고려해야 한다.

따라서 nominal 차이 감소를 “pointing이95%의 질량효과를 일으켰다”로 계산하면 안 된다. 서로 다른 부분 표본이고, 상관된 다른 기하가 동시에 달라졌을 수 있다. Pointing127쌍에서 차이가0과 양립한다는 결과는 **그 부분 표본에 대한 관측**이다.

NPR는 모든 입력·점수·그룹·matching을 동일하게 계산했다. Pointing0.05의114쌍은 rawSMD0.125로 기준 실패,0.025의84쌍은 통과했다.84쌍의 중앙 반응차이+3.600pp[−10.791,+17.902]로 확정적 차이는 없다. PR의127쌍을 NPR의균형통과대조군처럼 취급하지 않았다.

#### 4. 점수 기여를 빼는 진단과 전체 폭

각 입력군의 SHAP항을 raw margin에서 빼고, 질량을 보지 않은 채 원래 BDT통과와 같은 가중생존율로 재선택했다. 이는 **수정된 점수에 대한 진단**이며 실제 feature 제거·재훈련·track기하 변경이 아니다.

PR after FWHM: 원래통과0.610, pointing항제외0.594, 비행거리항제외0.554, 두항함께제외0.560, topology항제외0.580MeV. **이 방식으로 좁은 core가 사라지지 않았다.** 단일 점수항을 제거하면 현상이 해결된다는 증거는 없으며, 상관된 입력과 달라진 선택집단을 무시한 BDT 수정은 정당화되지 않는다.

PR 통과의 경험적 width68은1.3045→1.4497MeV다. 증가+0.1451MeV의2,000event-bootstrap구간[−0.0190,+0.2753]은0을 포함한다. 따라서 “전체 분해능이 유의하게 좋아졌다”도, “전체 폭의 악화가 이번검사만으로 확정됐다”도 아니다. FWHM이 좁은 core를 기술한다는 것과 전체68%폭이 개선됐다는 것은 별개다.

#### 5. 검증, 오차 및 남은 범위

- Nominal DSCB200fits 모두status0;82개는 적어도한parameter가설정경계에근접. 기존12개baseline FWHM을정확히재현했다.
- 독립Python likelihood 최대차이2.661e−11, 수치적분 정규화검사 및 FWHM 항등식 통과. `fit_validation.json` 참조.
- 고정bin·score기여·matching검사와width68:2,000event-Poisson bootstrap,seed2026092942. 같은event의후보는같은배율을받는다. Source/family/event identity를유지했다.
- PR세비교 FWHM:300event-Poisson replicas,12nominal포함3,612fits. 처음status비정상29개를Strategy2·150,000call·추가초기값으로재검사.21개가남았고,최종2,088fits는경계접촉이있다. 수렴한replica만사용한95%구간도 원래[−0.938,+0.478],pointing[−0.868,+0.736],daughter[−0.745,+0.598]로모두0포함. 실패를숨기거나구간을정밀검증된coverage로주장하지않는다.
- Retry29fits의독립likelihood 최대차이2.501e−12. 최초결과와retry·최종표를모두보존했다.
- Matching은bootstrap마다다시추정하지않았다. 구간은 **고정된matching·모델·weight에조건부**다. 모델훈련,matching추정,sourceweight추정불확실성을포함한전체오차가아니다.
- 중앙bin과matching거리·균형기준은질량결과에맞춰최적화하지않았다. 여러caliper·입력군결과를전부보존하고작아지는결과만골라보고하지않았다.

**현재 완료:** 실제배포입력/모델재현,입력군별점수분해,같은후보의질량반응과연결,공통표본matching,전후DSCB·width68·중앙bin,bootstrap및plot/report.

**미확정:** 특정입력조합이전체질량효과를설명하는인과분율. 공통표본과추정정밀도가부족하여이번자료만으로완결할수없다. BDT재훈련·새MC생성·production변경·DATA재처리는하지않았다.

추가검증을진행한다면 현재후보로는이전실제fitter기하개입의paired결과를BDTpointing/유의도구간별로보고,그후다른cosbin또는독립MC에서미리정한비교를검증하는것이맞다. 단순SHAP항제거를production처치로이식하거나,특정FWHM을만들도록covariance를조정하는것은이번결과로정당화되지않는다.

#### 외부 근거와 적용 범위

- [XGBoost 공식 Python API, Booster.predict의pred_contribs](https://xgboost.readthedocs.io/en/release_1.7.0/python/python_api.html#xgboost.Booster.predict): “feature contributions (SHAP values) for that prediction.” 이번기여분해의대상은예측점수다. 물리적질량변화의인과분해라는주장은문서에서나오지않는다.
- [Matchev–Shyamsundar, arXiv:1911.12299, Abstract](https://arxiv.org/abs/1911.12299): “prevents the background distribution from becoming peaked.” 선택과관측량의상관이shape를바꿀수있다는일반적동기를제공한다. 논문의배경질량설명을이번signal-refit오류의증명으로사용하지않았다.

#### 산출물 및 재현

- `attribution_and_matching.png/.pdf`: 실제점수기여와PR세비교의FWHM변화차이/95%bootstrap구간.
- `matched_mass_shapes.png/.pdf`: 원래표본및pointing127쌍의동일후보전후질량;통과/탈락별분포·DSCB. 오차막대는event-cluster delta-method 정규화histogram오차다.
- `model_validation.json`, `analysis_validation.json`, `fit_validation.json`, `bootstrap_validation.json`.
- `extracted.json`, `analysis.npz`, `matched_pairs.csv`, `group_attribution.csv`, `matching.csv`, `balance.csv`, `shape_metrics.csv`, `width_bootstrap.csv`, `fwhm_interaction.csv`.
- `model.py`:기존`d0_training_py310/venv/bin/python`으로실행. `analyze.py`도같은환경,`OPENBLAS_NUM_THREADS=2`로실행했다. `extract.py`, `check.py`, `prepare_bootstrap.py`, `summarize_bootstrap.py`, `plot.py`는일반python3. `prepare_bootstrap.py`의재실행이실제사용한두bootstrap입력파일의해시를정확히재현하는것도확인했다.
- ROOT는`/software/ROOT/ROOT-v6.24/root-6.24-install/bin/thisroot.sh`를source하고 `root -l -b -q 'feature_fit.C("절대study경로")'`, `case_bootstrap.C`, `retry_bootstrap.C`를**해석실행**했다. 컴파일하지않았다.
- 불완전원격작업을추가제출하지않았고기존자료만읽었다. Production코드·모델·가중치파일은변경하지않았다.
<!-- source:SRC-15:end -->

<a id="src-16"></a>

## SRC-16 — bdt_refit_causality_20260929/embedded_truth/PLAN.md

**기록 구분:** 계획·제안 원문 (완료의 증거 아님). **원문:** [bdt_refit_causality_20260929/embedded_truth/PLAN.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/embedded_truth/PLAN.md). **SHA256:** `0a7fd3eaf3703e97ce0f09a3a3bc94ce2773c548cbeb88ddac231cc0fb849604`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-16:start -->
### Recover the embedded signal reference from retained MiniAOD

Done: use retained background GEN ancestry plus actual simulation source to define/audit the embedding translation; evaluate saved input/fitted slow-pion states at the same recovered production point with the actual CMSSW magnetic field; compare angular residuals, fitted-vertex displacement and mass bias/core/tails by frozen BDT groups. Report inference versus independent validation explicitly and count all ambiguous/missing events.

Scope: existing1,264validated official target candidates; no new MC generation, BDT training or canonical production changes. Study-only analyzer; no D0/D* reconstruction repeated. Exact recorded source/event/GEN-index identities and weights. No reco-PV substitution or modal-vertex fallback. No calibrated pull claim without validated covariance.

1. Read actual SIM release source (Hydjet background vertices, MixEvtVtxGenerator, GenParticleProducer), recover a unique background root vertex from retained motherless background particles and audit uniqueness/ancestry. Preserve rejection reasons.
2. Create EmbeddedTruthProbe.cc and replay_cfg.py inside study; compile the CMSSW plugin in the existing isolated study runtime (not a ROOT macro). Preflight the previously inspected event and verify saved-state propagation against previous results.
3. Use existing authorized CERN Condor workflow for small independent event-read jobs, capped concurrency16, with lightweight analyzer only. Record cluster, state/provenance, all failures and terminal outputs; monitor completion. No shell background processes.
4. Analyze paired vertex/angular/mass quantities with uproot/numpy, create inspected CMS plots and report, verify destinations/hashes and mirror the study report. The recovered reference is source-derived, not a claim that missing original VtxSmeared products were recovered.

#### Verified checkpoint — 2026-09-29T06:06:08.689357+00:00

- Analyzed 1229 candidates from 61/63 completed jobs; prompt716 fully covered. All accepted references recovered, background-root spread0, old-reference propagation reproduction difference0. Frozen identities/weights and unchanged production source hashes pass.
- Remaining jobs: NPR_15, NPR_24. NPR_24 failed with FileOpenError on an unavailable FNAL source (17 targets). NPR_15 remained running with slow FNAL/CIEMAT reads (18 targets) at this checkpoint; it was not stopped. The earlier NPR_26 exclusion is a separate17targets.
- Four inspected PNG/PDF figures, measured tables and report are written. This is a completed analysis of the accepted snapshot, not a claim of full job coverage or full reconstruction validation.
- Resume: run collect_remote.py on lxplus; fetch records.json; then analyze.py, validate_analysis.py, write_report.py, update_parent.py and plot.py locally. Do not combine unchecked partial job output or change the frozen target set.
<!-- source:SRC-16:end -->

<a id="src-17"></a>

## SRC-17 — bdt_refit_causality_20260929/embedded_truth/REFERENCE.md

**기록 구분:** 정의·설계·재현 자료 (각 본문의 구현 상태 참조). **원문:** [bdt_refit_causality_20260929/embedded_truth/REFERENCE.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/embedded_truth/REFERENCE.md). **SHA256:** `d726af1c29b32d300a2ea1d1ebe6748aaacbf73cbdd2100c777b8a88f326d854`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-17:start -->
### Recovering the signal production reference

The absence of a persisted VtxSmeared HepMC product does **not**, by itself, establish that the reference is unrecoverable from MiniAOD. This study explicitly tests retained GEN ancestry instead of stopping at the product inventory.

#### Source chain

The inspected official file's actual SIM release is CMSSW_13_0_23_HeavyIon. Its recorded VtxSmeared module is MixEvtVtxGenerator, with signalLabel=generator:unsmeared and mixLabel=mix:generatorSmeared. The actual tracked configuration is preserved in ../geometry_selection/input_provenance_all.txt.

1. [Hydjet2Hadronizer::get_particles](https://github.com/cms-sw/cmssw/blob/CMSSW_13_0_23_HeavyIon/GeneratorInterface/Hydjet2Interface/src/Hydjet2Hadronizer.cc) creates hard subevent vertices at `(0,0,0,0)` and sets the first as the signal-process vertex. The corresponding [HydjetHadronizer](https://github.com/cms-sw/cmssw/blob/CMSSW_13_0_23_HeavyIon/GeneratorInterface/HydjetInterface/src/HydjetHadronizer.cc) has the same origin construction. Background vertex smearing translates these origins together. This source behavior motivates recovering that origin from retained incoming nucleons of the first background subevent, not from an arbitrary stable decay particle.
2. [MixEvtVtxGenerator::getVertex/produce](https://github.com/cms-sw/cmssw/blob/CMSSW_13_0_23_HeavyIon/SimGeneral/MixingModule/plugins/MixEvtVtxGenerator.cc#L79-L161) selects `const HepMCProduct& bkg = mix.getObject(1);`, reads its signal-process vertex, copies the unsmeared signal and calls applyVtxGen. No momentum boost is applied in this producer. The detector-frame signal position is therefore its unsmeared position plus this background shift.
3. [GeneratorMix_cff.py](https://github.com/cms-sw/cmssw/blob/CMSSW_13_0_23_HeavyIon/Configuration/StandardSequences/python/GeneratorMix_cff.py#L4-L11) sets genParticles.useCrossingFrame=True. [GenParticleProducer](https://github.com/cms-sw/cmssw/blob/CMSSW_13_0_23_HeavyIon/PhysicsTools/HepMCCandAlgos/plugins/GenParticleProducer.cc) reads the original crossing-frame objects; non-HI signal receives collisionId0 and advances the subevent offset by1. The first background subevent receives collisionId1. Particle production vertices are copied with mm-to-cm conversion. Stored signal GEN coordinates can consequently remain unsmeared even when the simulated signal was translated.

The entire background generator configuration was not retained in the inspected tracked signal metadata. The use of the first background root vertex is a source-derived recovery supported by its ancestry and event-wise consistency; it is not a direct comparison to a persisted VtxSmeared value. Keep that evidence distinction in conclusions.

#### Explicit recovery and rejection conditions

- Require matched D*/slow-pion GEN identities and their direct decay relation; both must be collisionId0. Compare the saved GEN momentum/vertex with the newly read GEN values (absolute difference<=1e-9).
- Select retained motherless particles with collisionId1. Require at least2roots and at least2incoming nucleons (absolute PDG2212 or2112).
- Require every selected root position to agree within1e-6cm in each coordinate. The first root position is used only after this unanimity check. It is not a mode or reconstructed PV estimate.
- Add this position to the signal slow-pion production vertex, including its nonzero B-decay displacement for nonprompt. Separately record the translated D* production vertex.
- Missing or ambiguous roots remain explicitly rejected. No fallback reference or missing-value imputation.

In the already-inspected target2201,4root particles including2nucleons agree exactly at(0.0379703492,-0.0184176359,1.8106344938)cm; reconstructed PV is a different record. The method reuses information actually present in MiniAOD.

#### Common-reference comparison

Saved input/fitted slow-pion seven-parameter states and covariances are restored in a study-only analyzer. The actual magnetic field uses the same132X_mcRun3_2023_realistic_HI_v9 GlobalTag as the previous production replay. TrackKinematicStatePropagator evaluates both states at the transverse PCA relative to the same recovered slow-pion origin. This is a common reference prescription, not a requirement that reconstructed trajectories pass exactly through truth.

The previous propagation to the fitted vertex is independently reproduced from the restored input state. Preflight target2201 agrees exactly; per-job tolerance is1e-6in the recorded momentum/position components, reflecting the float-valued state representation. This is not a new fit or change to the covariance prescription.

GEN momentum is compared to both propagated momenta; wrapped phi residuals, eta residuals and their combined distance are reported. D* fitted-vertex residuals use the translated D* production point. Common-reference Delta-m uses each stage's D0 four-vector and the propagated slow-pion momentum with that stage's mass. It is a truth-assisted diagnostic, not an observable available in DATA and not a proposed production correction.

No state-change pulls or coverage claim are made from indefinite/uncalibrated covariance. Event-cluster linearized standard errors are used only for paired mean absolute-error changes, with original weights held fixed; descriptive quantile widths are not assigned Gaussian error bars.
<!-- source:SRC-17:end -->

<a id="src-18"></a>

## SRC-18 — bdt_refit_causality_20260929/embedded_truth/REPORT.md

**기록 구분:** 조사 보고서·해석 (표본·시점별 결과). **원문:** [bdt_refit_causality_20260929/embedded_truth/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/embedded_truth/REPORT.md). **SHA256:** `4a6169c40de8263462e9afdafcf345a2fead8cdab87605492d4001d5a714880a`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-18:start -->
### Embedded-reference vertex, angular and mass diagnosis

#### Physical conclusion

**The narrow prompt BDT-pass core is strongly sensitive to the fitted-vertex evaluation point; it is not evidence of improved overall mass resolution.** For the 491 prompt pass candidates, transporting the *same fitted states* to the recovered production reference changes the fixed central-bin fraction from 19.791% to 13.405% (paired difference 6.386 ± 1.788 percentage points). Fitted D* vertex distance from that reference has a weighted median of 1.146 mm, whereas its transverse component has median 24.1 micrometres. This agrees with the weak longitudinal localization expected from the observed nearly parallel input trajectories.

At the same recovered origin, the slow-pion mean angular distance from GEN decreases from 5.580 to 4.837 in units of 1e-3. Thus the data do not support a blanket claim that the fitted slow-pion direction is worse. **But its individual direction accuracy is different from the D0–pion relative-angle accuracy, which enters the invariant mass.** The original-reference opening-angle mean absolute error changes by +0.371 ± 0.131 mrad; at the common origin it changes by +0.034 ± 0.041 mrad. The latter does not demonstrate an improvement.

Original mass-residual width68 is 1.3045 → 1.4486 MeV. Common-reference width68 is 1.3044 → 1.3702 MeV: the reference change reduces, but does not eliminate, the observed broadening. This is a controlled diagnostic of the reference dependence with fitted states held fixed; it does not independently prove that the propagator is faulty or establish a unique BDT-input cause.

The reconstruction is therefore **not validated as an overall mass-resolution improvement by the narrow fitted FWHM**. The measured mechanism is vertex-dependent angular evaluation and a changed relative opening angle, not a change in surviving candidates caused by applying the BDT threshold. No production replacement is justified solely by the truth-assisted curve. The source-derived origin qualification below applies to these comparisons.

#### What was actually done

**The retained MiniAOD information was used, not just inventoried.** The source-defined embedding translation is reconstructed from the unanimous production point of motherless first-background-subevent particles, requiring at least two nucleons. Signal GEN coordinates are translated by this point. Saved input/fitted slow-pion states are then transported with the actual CMSSW field to the same recovered production reference.

Current coverage: **1229 accepted candidates**, 61/63 completed event-read jobs, 2 not yet accepted jobs; 0 explicitly ambiguous references. The prior geometry input comprises 1,264 candidates (all 716 prompt, 548/565 nonprompt); the earlier 17 NPR_26 missing candidates are not in this run. Original weights, families, candidate identities, cosine assignments and MVA groups remain fixed. Partial coverage must not be represented as full production validation.

Unaccepted jobs in this snapshot: NPR_15, NPR_24. NPR_24 encountered FileOpenError / Socket timeout on FNAL file 2520000/61b91382-fc84-4dd2-95b5-04d64a4e756d.root and exited84. Its17targets are not accepted from the failed partial run; NPR_24_failed_run.log preserves the error. records.json contains the exact current queue state, so completed physics analysis and unresolved input access are distinguishable.

Official pThat2/pT4, pT7–10GeV/c, |y|<0.3, centrality0–10%, 0.6<|cos(theta_trk*)|<0.8, DCA0–0.08cm; fail 0<=MVA<0.95, pass MVA>=0.95. No reconstruction is repeated and no new MC generated. No production source or BDT changed.

#### Evidence level of the recovered reference

[REFERENCE.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/embedded_truth/REFERENCE.md) traces actual SIM13_0_23_HeavyIon metadata, background origin construction, MixEvtVtxGenerator translation and crossing-frame GEN storage. The signal detector point is reconstructed as stored_signal_vertex+background_root_vertex. Both prompt and nonprompt retain their own signal displacement. The reference is not a reco-PV substitution or an arbitrary histogram mode. Ambiguous roots are rejected explicitly.

This is a **source-derived recovery supported by retained ancestry and event-wise agreement**, not a direct comparison to an independently persisted VtxSmeared/simulation-vertex branch. That branch is absent. This distinction remains relevant to absolute truth claims; its absence alone was not a valid reason to stop the available comparison.

Maximum background-root coordinate spread: 0 cm. Restored-state propagation to the previous fitted reference reproduces the saved result to 0 in recorded momentum/position components (check tolerance1e-6). The preflight result was exactly equal. The same 132X_mcRun3_2023_realistic_HI_v9 GlobalTag and CMSSW_13_2_11 propagator as the original study are used.

#### Angular residuals at the same reference

Both momenta are evaluated at the transverse PCA relative to the recovered slow-pion origin; the reconstructed trajectory need not pass exactly through that point. Residuals use the matched GEN slow-pion momentum and wrapped phi. Combined angular distance is sqrt(deta²+dphi²). The table reports fixed-weight paired changes; N is candidates and Neff is a weighted precision diagnostic.

|Group|N|Neff|phi_RMS_mrad|eta_RMS_1e3|mean_DR_change|angular_improved|
|---|---|---|---|---|---|---|
|PR fail|225|225.0|6.6782 → 6.1607|6.0230 → 6.0578|-0.5460 ± 0.1478|63.1%|
|PR pass|491|485.8|6.3593 → 6.1382|5.6630 → 5.4503|-0.7434 ± 0.0923|70.3%|
|NPR fail|162|159.5|8.1261 → 7.9024|6.2067 → 6.8547|-0.1248 ± 0.2194|61.6%|
|NPR pass|351|345.6|6.5829 → 6.0233|6.8824 → 6.1187|-0.8836 ± 0.1232|73.3%|

Mean-DR-change units are 1e-3. The quoted error is an event-cluster linearized standard error for the paired mean, holding original weights fixed; it is not covariance/pull coverage or an exact confidence interval. RMS can be tail-sensitive. Candidate-level eta,phi,combined-distance and pT errors are all retained in candidates.csv; no single metric is selected to declare the reconstruction valid.

Opening-angle residuals use alpha=atan2(|p_D0 cross p_pi|,p_D0 dot p_pi) relative to matched GEN. The mass relation M(D*)²=M(D0)²+m_pi²+2(E_D0 E_pi−|p_D0||p_pi|cos(alpha)) makes clear why an improved single-track angular distance does not guarantee improved Delta-m. Both momenta and their relative errors matter; D0's own invariant-mass distribution can remain almost unchanged.

#### Fitted D* vertex relative to recovered production point

|Group|distance_median_um|distance_abs95_um|longitudinal_abs95_um|transverse_median_um|
|---|---|---|---|---|
|PR fail|1319.03|5233.93|5230.31|35.05|
|PR pass|1146.10|5742.80|5742.73|24.10|
|NPR fail|1143.06|4627.47|4627.33|29.22|
|NPR pass|1192.10|5897.84|5897.77|24.52|

Longitudinal/transverse are defined relative to the GEN D* momentum. The prior geometry study found nearly parallel D0/slow-pion trajectories and a weakly constrained common vertex along their flight direction. These are position residuals, not significance pulls; reported fit covariance has not been established to have correct coverage.

#### Mass bias, core and tails: original versus common reference

“Original” means the saved internal mass definition M(D0+slow)−M(D0) at its original input/fitted evaluation points. “Common” means the same stage's D0 four-vector plus the slow-pion momentum evaluated at the recovered GEN reference. D0 is neutral; its input/fitted momentum is kept as recorded. Mass hypothesis and same-stage D0 subtraction are held consistent. **Common-reference mass uses truth information and is not a proposed DATA observable.**

width68=Q84−Q16 in MeV. It measures the full central68% span and is not fitted FWHM.

|Group|original_width68|common_width68|original_mean_abs_change|common_mean_abs_change|fitted_reference_abs_cost|
|---|---|---|---|---|---|
|PR fail|1.2746 → 1.4587|1.2739 → 1.3873|+0.0752 ± 0.0263|+0.0198 ± 0.0110|+0.0554 ± 0.0242|
|PR pass|1.3045 → 1.4486|1.3044 → 1.3702|+0.0357 ± 0.0183|+0.0067 ± 0.0064|+0.0290 ± 0.0174|
|NPR fail|1.4430 → 1.5316|1.4426 → 1.5116|+0.0508 ± 0.0317|+0.0202 ± 0.0092|+0.0588 ± 0.0293|
|NPR pass|1.4078 → 1.3101|1.1649 → 1.2419|-0.0036 ± 0.0289|+0.0166 ± 0.0105|+0.0453 ± 0.0206|

The last column is mean(|original fitted mass residual|−|common-reference fitted mass residual|), in MeV. It isolates the change associated with the evaluation reference while retaining the same fitted D0 state; it is a diagnostic, not an independent causal decomposition of the full fitting procedure. Widths have no newly estimated uncertainty; paired mean errors use event clusters.

|Group|Reference|bias_MeV|central_percent|abs_residual_gt2_percent|
|---|---|---|---|---|
|PR fail|raw|0.3144 → 0.3448|16.0000 → 15.5556|8.8889 → 12.4444|
|PR fail|common|0.3144 → 0.3425|16.0000 → 15.1111|8.8889 → 9.3333|
|PR pass|raw|0.2033 → 0.2327|15.2590 → 19.7911|8.2548 → 8.0488|
|PR pass|common|0.2033 → 0.2174|15.4650 → 13.4049|8.2548 → 7.8428|
|NPR fail|raw|0.2508 → 0.2913|12.0054 → 15.6991|9.4194 → 10.6754|
|NPR fail|common|0.2574 → 0.2608|18.8389 → 20.7228|8.7915 → 8.7915|
|NPR pass|raw|0.1937 → 0.2765|11.0200 → 15.9843|9.6043 → 10.4743|
|NPR pass|common|0.2107 → 0.2403|14.2443 → 14.2443|9.0243 → 9.0243|

The central bin is frozen at 145.288135593–145.508474576MeV, the same as the previous study. It is not aligned separately to each fitted or GEN peak. No new DSCB fit is used to manufacture a narrower core. A central-bin gain, full-width change and truth residual improvement answer different questions.

#### Scope of the physical verdict

The new comparison tests whether the recorded fitted track is more accurate when input and fitted states are compared at the **same** recovered origin, and whether mass changes are sensitive to a poorly localized fitted vertex. It must not be reduced to “the refit is correct” merely because one residual metric improves. Conversely a wider original Delta-m distribution is not, by itself, proof that the trajectory transport or the fitted-state update is wrong.

The earlier BDT-specific core contrast has 95% paired-bootstrap intervals containing zero; the present descriptive scans do not supersede that statistical result. No unique BDT-input cause, error coverage, full DATA/MC closure or yield bias is established here. Production treatment requires those distinctions; truth-assisted recomputation is not a deployable correction.

The next deployable comparison is the current reconstruction versus the pre-D*refit mass definition, using identical selections and efficiency bookkeeping, followed by signal-template and yield closure. Any vertex constraint must respect displaced nonprompt production; replacing all vertices with the reco PV or GEN origin would not be a justified universal treatment. A hypothesis that BDT selects *larger* vertex errors is not supported by prompt pass/fail median distances alone; signed geometry and candidate selection remain distinct from error magnitude. The earlier posdef intervention did not remove the effect and this study did not change the covariance prescription.

#### Execution and artifacts

EmbeddedTruthProbe.cc is a study-only CMSSW plugin. Initial preflight failed before processing because the plugin cache had not refreshed after the package build; a full isolated-runtime registration step fixed it. preflight_v2 succeeded, exactly reproducing the earlier propagation. No ROOT macro was compiled and no long job was killed.

CERN Condor cluster 12767477, 63 jobs, max_materialize 16,source/event-only reads. worker.sh validates the relocated runtime, exact target sets, GEN identity and state propagation, and transfers per-job records/logs. records.json records accepted jobs and pending/held queue states. Original hashes and preparation receipts preserve runtime provenance. Common-reference values are computed for successfully recovered targets only; missing/ambiguous cases are explicitly recorded in audit.json.

NPR_06 initially exited with exit code 134 (SIGABRT) after closing its last input. Its first outputs/logs are retained remotely under failed_attempts/NPR_06_first. A single identical retry succeeded; the failed attempt was not silently accepted. production_hash_validation.json verifies the D0Fitter, DStarFitter and TrackAndVertexUnpacker study-runtime sources against their prior hashes. validate_analysis.py additionally checks exact target coverage, frozen weights and stage identities.

Figures: common_reference_angles, vertex_residual, mass_reference_comparison, mass_shape_reference (PNG/PDF). Analysis tables: candidates.csv,summary.csv; summary contains N,sumw,sumw2,Neff and event count. Source algorithm and truth-reference qualification: REFERENCE.md.

#### Verified checkpoint — 2026-09-29T06:06:08.689357+00:00

- Analyzed 1229 candidates from 61/63 completed jobs; prompt716 fully covered. All accepted references recovered, background-root spread0, old-reference propagation reproduction difference0. Frozen identities/weights and unchanged production source hashes pass.
- Remaining jobs: NPR_15, NPR_24. NPR_24 failed with FileOpenError on an unavailable FNAL source (17 targets). NPR_15 remained running with slow FNAL/CIEMAT reads (18 targets) at this checkpoint; it was not stopped. The earlier NPR_26 exclusion is a separate17targets.
- Four inspected PNG/PDF figures, measured tables and report are written. This is a completed analysis of the accepted snapshot, not a claim of full job coverage or full reconstruction validation.
- Resume: run collect_remote.py on lxplus; fetch records.json; then analyze.py, validate_analysis.py, write_report.py, update_parent.py and plot.py locally. Do not combine unchecked partial job output or change the frozen target set.
<!-- source:SRC-18:end -->

<a id="src-19"></a>

## SRC-19 — bdt_refit_causality_20260929/geometry_selection/PLAN.md

**기록 구분:** 계획·제안 원문 (완료의 증거 아님). **원문:** [bdt_refit_causality_20260929/geometry_selection/PLAN.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/geometry_selection/PLAN.md). **SHA256:** `e8977d1fca84fa95845dd0aeb76baab1a43f1cc19be8a67bed54bd2ca53bd88f`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-19:start -->
### Geometry selection and truth-reference follow-up

Done: identify signed Delta-m migration versus saved D0/slow-pion geometry; distinguish population selection, calculation identities and causal interpretation; audit the signal GEN-to-detector vertex mapping from retained products and actual production code. Produce reproducible candidate tables, plots and a report, explicitly retaining unresolved truth-reference limitations.

Scope: fixed official PR/NPR pT4 candidates, pT 7–10 GeV/c, |y|<0.3, centrality 0–10%, 0.6<|cos(theta_trk*)|<0.8, DCA 0–0.08 cm. Original MVA membership and weights. No canonical production edits, new MC generation, new BDT training or automatic covariance treatment.

1. Read existing geometry records, original NPZ, manifests and CMS propagation/embedding code. Inspect SSH/runtime and saved GEN-origin products.
2. Add `analyze.py`: exact index join, signed propagation/opening-angle identities, central-bin entry/exit classification, geometry distributions and explicit common-support checks. Use numpy; no ROOT compilation or PyROOT. Save candidate CSV/NPZ and numeric summaries.
3. Add `plot.py`: CMS Simulation Internal PNG/PDF, selected geometry and signed migration. Inspect rendered images.
4. Audit vertex-frame provenance with read-only lxplus commands and, only if required, a small interpreted FWLite inspection of existing MiniAOD. Never substitute reco PV as truth.
5. Run invariant/identity and destination checks, write `REPORT.md` and update the parent report/status with physical conclusions and limitations. No new significance claim from descriptive scans; retain the earlier paired-bootstrap result.

Commands: python3 geometry_selection/analyze.py; python3 geometry_selection/plot.py; read-only ssh lx; root -l -b -q for any documented small FWLite audit. Stop expanding remote inspection if missing persisted truth/embedding products prevent an identifiable reference; state the exact missing evidence.

#### Outcome

Steps1–3 completed. Step4 established actual SIM release/configuration and one-event background/signal frame separation; exact per-event simulation translation is not independently retained, so full angular truth validation remains unresolved. Step5 completed with invariant/identity checks and explicit limitations. Added a strict0.2pooled-SD caliper comparison to evaluate coarse-matching limitations; no outcome-based matching choice. No production edits or new background jobs.
<!-- source:SRC-19:end -->

<a id="src-20"></a>

## SRC-20 — bdt_refit_causality_20260929/geometry_selection/REPORT.md

**기록 구분:** 조사 보고서·해석 (표본·시점별 결과). **원문:** [bdt_refit_causality_20260929/geometry_selection/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/geometry_selection/REPORT.md). **SHA256:** `a7418711ea2724eae6d2e1249ab84f320b1b0083a0ab3a74b1f5466595fb637d`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-20:start -->
### Geometry selection, signed mass migration and GEN frame audit

#### Physical conclusion

**Confirmed:** rotating only the slow-pion phi reproduces the full propagation contribution to Delta-m, with maximum difference **6.32e-6 MeV** across1,264 candidates. The *pre-fit* D0 and slow-pion input lines already predict the sign of this change for **96.5%** of weighted prompt BDT-pass candidates (95.1% of fail). This establishes a geometric charged-track transport path; it is not merely a correlation with the final fitted vertex.

**Confirmed:** prompt pass candidates do not all move toward the peak. During propagation50 enter the fixed central bin and33 leave: weighted fractions10.3002% and6.7981%, giving+3.5021percentage points. Only46.7776% move closer to the fixed bin center; only46.1743% improve their absolute GEN mass residual. At the final fitted-state stage53 enter and31 leave (+4.5321pp). Net central concentration therefore coexists with overall broadening. Candidate counts and weighted percentages are distinct.

**Interpretation:** D0 and slow pion are almost parallel. Their common vertex is weakly constrained along their flight direction; evaluating the charged slow-pion momentum at that position changes phi, the opening angle, and Delta-m. This mechanism operates in both BDT groups. BDT selects different D0 topology, but no topology variable has been isolated as the cause of the extra prompt-pass core enhancement.

**Unresolved:** the BDT-specific contrast is not established statistically: the earlier event-bootstrap95% intervals include zero. These descriptive scans do not override that result. No BDT error, incorrect field transport, or uniquely responsible covariance defect is established. Narrower DSCB FWHM is not demonstrated improvement of overall mass resolution.

#### Frozen sample and definitions

Official prompt/nonprompt pThat2/pT4; reconstructed D* pT7–10GeV/c, |y|<0.3, centrality0–10%, tracker-axis0.6<|cos(theta*)|<0.8, DCA0–0.08cm. Original identities, families, weights, cosine assignments and BDT groups remain fixed. All accepted scores are>0; fail is0<MVA<0.95 and pass isMVA>=0.95. Geometry coverage: all716prompt (225fail,491pass),548/565nonprompt. NPR_26's17 missing candidates stay excluded.

New stage comparisons consistently use M(D0+slow)−M(D0), subtracting the D0 mass from the same stage. The central bin is145.28813559322035–145.5084745762712MeV; its center is a fixed diagnostic value, not every candidate's GEN mass. Nominal saved after-refit mass has a slightly different D0 subtraction and is not substituted silently. This explains internal after width1.448572 versus nominal1.449653MeV for prompt pass.

Index8051 (NPR pass) has NaN decay-length significance. It remains in all other comparisons and is omitted only for that variable's summaries/matching; per-metric N and audit record it. No imputation. Index10257 has a nonpositive fitted vertex covariance and remains flagged. All prompt vertex covariance matrices in this snapshot are positive definite, which alone does not validate their error coverage. No new production, BDT training, DSCB fit, or bootstrap was run.

#### Signed geometry and mass identity

Hold D0 and slow-pion energies,pT,pz fixed. With Delta=phi_slow−phi_D0 and delta=the slow-pion propagation rotation:

`M_new²−M_old² = 2 pT_D0 pT_slow [cos(Delta)−cos(Delta+delta)]`.

Dividing by M_new+M_old gives the exact Delta-m shift, since the D0 mass subtraction stays fixed. Candidate-wise substitution reproduces the full propagation step to6.32e-6MeV. This checks the propagation component, not the remaining fitted-state update or the causal origin of the fitted endpoint.

For small rotations, delta(Delta-m)≈(pT_D0 pT_slow/M_Dstar)sin(Delta)delta_phi. In a positive approximately longitudinal field, delta_phi≈−q k Bz sT/pT_slow, with k=0.00299792458 for cm and GeV/c. The sign depends on **charge × signed path × relative azimuth**, not just pointing or propagation distance. The exact trigonometric formula is used for closure; linearization is inaccurate for some tails.

For a pre-fit predictor, use the local tangent lines of the D0 and slow-pion input states, solve their closest-approach pair, and project the slow-pion extrapolation into the transverse plane. Define H=−q sT_line sin(Delta). It uses neither fitted vertex nor fitted momentum, and is a straight-line diagnostic rather than an alternative reconstruction.

|Family/group|N|Weighted sign agreement H vs mass shift|Weighted correlation|Median opening angle (rad)|Median major vertex scale (mm)|Major-axis alignment with D0|
|---|---:|---:|---:|---:|---:|---:|
|PR fail|225|95.11%|0.9711|0.05957|1.772|0.9999957|
|PR pass|491|96.50%|0.9606|0.05671|1.853|0.9999969|
|NPR fail|174|94.72%|0.9902|0.05810|1.796|0.9999953|
|NPR pass|374|95.65%|0.9817|0.05518|1.840|0.9999969|

The major covariance axis is almost parallel to the D0 direction. The roughly1.8mm reported scale and0.057rad opening angle support weak longitudinal localization of this two-trajectory intersection. These are fit-output geometry diagnostics, not validated Gaussian uncertainties or measured vertex residuals to truth.

#### Entry/exit and matching

Prompt-pass propagation entrants versus exits have weighted medians: pointing0.02459 vs0.03016rad; decay-length significance7.606 vs7.473; input-line separation71.45 vs47.62micrometers. The distributions overlap. Entry/exit is defined by the outcome, so conditional differences do not establish causation.

Pooled weighted-tertile comparisons require>=5original candidates of each group per cell; both groups receive the smaller total cell weight. Empty support is explicitly excluded. Continuous matching additionally pairs candidates without replacement, requiring each pre-fit feature within0.2pooled weighted SD, choosing the smallest feature distance first. Pair weights=min(original weights). No fit outcomes choose the pairing.

|Prompt comparison|Retained fail/pass|Max residual abs(SMD)|Pass−fail propagation gain after adjustment|
|---|---:|---:|---:|
|Tertiles: input-line separation+opening|225/491|0.1105|+3.580pp|
|Tertiles: pointing+decay significance+line separation+opening|12/14|0.8119|+14.286pp|
|Caliper: pointing+decay significance|79/79|0.1321|+3.843pp|
|Caliper: all four geometry variables|28/28|0.5978|−7.143pp|
|Caliper: initial mass+pre-fit signed geometry H|195/195|0.0040|+2.564pp|

The four-variable matched samples are too small/unbalanced to represent the full population; the changed signs are not a causal result. Initial mass+H matching has good mean balance but only195/491pass candidates and no new uncertainty estimate. It neither establishes the residual contrast nor proves geometry is the complete cause. Same-support before/after numbers, N,sumw,sumw2,Neff, matching balance and exact pair identities are saved in CSVs. No outcome-selected preferred matching method.

#### Mass truth

For all491prompt pass candidates, internal GEN-residual width68=Q84−Q16 changes **1.304504→1.444778→1.448572MeV** for input→propagated→fitted states. Mean absolute residual grows by0.02686MeV during propagation. These mass comparisons require no spatial GEN translation. Concentration in one core bin therefore does not establish improved overall resolution. Full family/group numbers and candidate/weight/Neff accounting are in summary.csv.

#### GEN frame: new evidence and remaining validation

Actual inspected MiniAOD processing: SIM **CMSSW_13_0_23_HeavyIon**, HLT/RECO/PAT **CMSSW_13_2_16_patch1**; study replay13_2_11. The actual file's input_provenance_all.txt identifies VtxSmeared as MixEvtVtxGenerator, signalLabel=generator:unsmeared, mixLabel=mix:generatorSmeared. Its mixing input includes generator:unsmeared. The matching SIM release's standard GeneratorMix configuration stores genParticles from the crossing frame; MixEvtVtxGenerator separately copies and translates signal to the background vertex for simulation.

**Direct event inspection:** run1/lumi1719/event111474631, target2201. D*/slow pion have collisionId0 and stored vertex(0,0,0). Background motherless particles with collisionId1 agree at(0.0379703492,−0.0184176359,1.8106344938)cm;39,204GEN particles share it. Reco PV is(0.0380752869,−0.0181232877,1.8105016947)cm. This supports unsmeared signal GEN stored alongside translated background GEN. It does **not** imply signal tracks were simulated at the origin.

**Evidence level:** source+event data strongly support detector_signal_vertex=stored_signal_vertex+background_embedding_vertex for this workflow. However, the original HepMC signal-process-vertex selector/output is absent from this MiniAOD. Untracked configuration and event-level parent dependencies are not recovered in the metadata dump; gen_dependencies.txt reports no recorded dependencies. A common/motherless background vertex is a candidate way to reconstruct the translation, not an independently verified per-event VtxSmeared product.

Therefore **full common-reference angular/vertex truth validation remains unresolved**. We established the storage/embedding mechanism and a concrete background-GEN reference for one event, but not its exact equivalence to the simulation embedding point for all1,264candidates. No modal GEN point, reco PV, or raw signal(0,0,0) is silently substituted as truth; no uncalibrated state-change pull is used. We do not claim completed angular truth or yield closure.

To settle this: compare the retained-background extraction with actual VtxSmeared/background HepMC or simulation vertices in a small matched sample, then translate each event and propagate both input/fitted slow-pion states to that common reference. Missing provenance/truth correspondence, rather than more mass fits or BDT retraining, is the remaining requirement. Large new production was not launched.

#### Sources

- [CMS vertex-refit guide, Introduction](https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideVertexFitTrackRefit#Introduction): parameters are “re-estimated at the fitted vertex”. General mechanism, not proof of this sample's BDT cause.
- [CMSSW13_2_11 stateAtPoint](https://github.com/cms-sw/cmssw/blob/CMSSW_13_2_11/RecoVertex/KinematicFitPrimitives/src/TransientTrackKinematicParticle.cc#L64-L70): calls propagateToTheTransversePCA.
- [Actual SIM release GeneratorMix_cff.py, lines4–11](https://github.com/cms-sw/cmssw/blob/CMSSW_13_0_23_HeavyIon/Configuration/StandardSequences/python/GeneratorMix_cff.py#L4-L11): “Produce GenParticles of the two HepMCProducts”; genParticles.useCrossingFrame=True.
- [Actual SIM release MixEvtVtxGenerator.cc, getVertex/produce](https://github.com/cms-sw/cmssw/blob/CMSSW_13_0_23_HeavyIon/SimGeneral/MixingModule/plugins/MixEvtVtxGenerator.cc#L79-L125): `const HepMCProduct& bkg = mix.getObject(1);`; lines143–161copy signal and apply the shift.
- [Actual SIM release GeneratorSmearedProducer.cc](https://github.com/cms-sw/cmssw/blob/CMSSW_13_0_23_HeavyIon/GeneratorInterface/Core/plugins/GeneratorSmearedProducer.cc#L29-L51): standard currentTag=VtxSmeared is copied into generatorSmeared.

#### Artifacts and verification

analyze.py: exact candidate joins, signed mass identities, migration accounting, weights/Neff, common-support and caliper comparisons. plot.py: five CMS Simulation Internal PNG/PDF pairs: signed_migration,prefit_geometry,entry_exit_geometry,topology_response,truth_width. inspectFrame.C: interpreted ROOT/FWLite read of one existing MiniAOD event, no ACLiC compilation. frame_event.json and actual production provenance preserve the coordinate audit.

Checks: exact index/weight/MVA identity; explicit missing-feature audit; straight-line separation identity; phi-only mass closure; entry/exit weighted-gain closure; pair uniqueness/calipers; Python syntax and output destinations. Only campaign study artifacts/report links changed; production source, BDT and samples remain unchanged. Remote reads finished; no new background jobs launched. See validation.json for final checks.
<!-- source:SRC-20:end -->

<a id="src-21"></a>

## SRC-21 — bdt_refit_causality_20260929/matched_shape_bootstrap/PLAN.md

**기록 구분:** 계획·제안 원문 (완료의 증거 아님). **원문:** [bdt_refit_causality_20260929/matched_shape_bootstrap/PLAN.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/matched_shape_bootstrap/PLAN.md). **SHA256:** `cd830bfb538436ad48ac30f0210ccad11e7ff4fb845810478d800dd111385443`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-21:start -->
### Matched candidate refit and BDT bootstrap

Done: compare identical weighted official pT4 PR/NPR candidates before/after refit, separately BDT fail/pass and inclusive; report DSCB FWHM, empirical Q84-Q16 and a fixed central-bin fraction with shared event bootstrap contrasts. Verify original input/source/weight identities and compare to prior plots. Produce checked PNG/PDF and numeric/report artifacts. No new MC, BDT training or production edits.

1. prepare.py: join requested candidates.npz to saved diagnostic states using source/task/event/candidate identity; freeze common before/after fit range 140–153MeV and report all exclusions. Save matched.npz, inputs.root, 400 common event-Poisson bootstrap draws, provenance.
2. bootstrap_fit.C: interpreted ROOT Minuit2, weighted unbinned finite-range DSCB with the existing six parameter ranges; common multistart strategy; 12 nominal fits and 400 paired bootstrap replicas. Record NLL, status, EDM, boundaries and failures. Validate direct PDF evaluation against StableCrystalBall.
3. analyze.py: event-paired before/after, pass-minus-fail and interaction contrasts; width68/central-bin metrics use the same bootstrap draws. Preserve inclusive-to-tight comparison as a separate nested contrast. Fixed central-bin edges145.28813559322035–145.5084745762712MeV from prior study, never optimized here.
4. Use saved slow-pion eta/phi and Delta-m shifts for score-binned correlation diagnostics. Investigate shape templates/yield closure only if the fixed-sample effect survives bootstrap and nonparametric width checks; otherwise report sample/model sensitivity without claiming a BDT error.
5. Draw 4-panel same-candidate distributions with CMS Simulation Internal style, inspect rendered outputs, and document source mismatch in supplied figures. Keep all intentional study artifacts; no arbitrary scratch files.

No ACLiC compilation; source ROOT6.24 and run root -l -b -q bootstrap_fit.C. Bootstrap errors condition on supplied physical weights and source selection; do not include production-wide nuisance uncertainty.
<!-- source:SRC-21:end -->

<a id="src-22"></a>

## SRC-22 — bdt_refit_causality_20260929/matched_shape_bootstrap/REPORT.md

**기록 구분:** 조사 보고서·해석 (표본·시점별 결과). **원문:** [bdt_refit_causality_20260929/matched_shape_bootstrap/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/matched_shape_bootstrap/REPORT.md). **SHA256:** `af27538ff2868bb53461611d8b8410e8af9c2b64d0b301977e14a18d3bc1ceac`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-22:start -->
### 동일 후보의 BDT 선택과 refit: event bootstrap 검증

**판정:** 같은 official pT4 후보에서도 prompt의 DSCB FWHM 점추정은 tight cut 이후 크게 작아진다. 그러나 empirical width68은 같은 감소를 보이지 않고, DSCB bootstrap 및 통과−탈락의 refit 반응 차이도 95% percentile 구간에서 0을 포함한다. 따라서 이번 결과는 강건하게 입증된 BDT×refit 이상이 아니다. 현재 우선 확인할 것은 DSCB core/tail 모형과 경계·표본변동에 대한 FWHM 민감도다. 실제 물리적 selection effect의 부재를 입증한 것도 아니다.

#### 표본 및 두 기존 그림의 차이

기존 pre-refit 그림은 official PR/NPR pThat2/pT4만 사용했지만 기존 refit 그림은 official/private 전체 source 혼합이었다. 그 둘의 FWHM 수치를 직접 전후 효과로 해석할 수 없다. 이번 결과는 사용자가 지정한 original_mass_mva_official4_20260929/candidates.npz에서 같은 source·후보·physical weight를 고정했다.

D* pT7–10GeV/c, |y|<0.3, centrality0–10%, tracker-axis |cosθ*|0.6–0.8, DCA0–0.08cm. 각 집단은 BDT 0<MVA<0.95 또는 MVA≥0.95. Inclusive MVA>0→tight는 별도 nested 비교다. Refit 이후 각 후보를 다시 빈으로 선택하지 않았다.

1,281개를 진단 후보와 모두 정확히 연결했다. 양쪽에서 동일한 140–153MeV fit 범위를 적용하기 위해 pre-refit=153.416MeV인 prompt fail 1개를 양쪽에서 제외했다. 최종1,280개/1,257이벤트: prompt fail224/pass491, nonprompt fail177/pass388. 원본은 수정하지 않았다. Exclusion identity와 SHA256은 provenance.json에 있다.

Before는 저장된 deltaMOriginal, after는 그림에서 실제 사용한 nominal mass−massDaugther1이다. 별도의 diagnostic deltaMRefit과 nominal 차이는 원래1,281개에서 −0.1269…+0.1509MeV이며 둘을 혼용하지 않았다.

#### 네 집단의 같은 후보 전후

DSCB FWHM/width68 단위MeV, 중앙비율 단위%. width68=weighted Q84−Q16. 중앙-bin은 기존 study의 [145.28813559322035,145.5084745762712)MeV로 고정했으며 fit mean 또는 새 그림의 최대 bin에 맞춰 이동시키지 않았다.

|Family / BDT|N|DSCB FWHM 전→후|width68 전→후|중앙비율 전→후|
|---|---:|---:|---:|---:|
|PR / fail|224|1.1913 → 1.1398|1.2742 → 1.4452|16.0714 → 15.6250|
|PR / pass|491|1.2747 → 0.6101|1.3045 → 1.4497|15.2590 → 19.9971|
|NPR / fail|177|1.1893 → 0.6458|1.4941 → 1.5479|11.0277 → 15.6423|
|NPR / pass|388|1.2491 → 1.0344|1.4513 → 1.3574|11.2877 → 15.7813|

모든 개별 지표의 event-bootstrap 표준오차와 구간은 metrics.csv에 있다. 후보수 N과 N_eff는 다르다.

|Family / BDT|N|sumw|sumw2|N_eff|100/√N (%)|100/√N_eff (%)|
|---|---:|---:|---:|---:|---:|---:|
|PR/fail|224|2170.997743|21041.210716|224.000|6.682|6.682|
|PR/pass|491|4704.731102|45560.825934|485.823|4.513|4.537|
|NPR/fail|177|140.053169|112.933476|173.685|7.516|7.588|
|NPR/pass|388|307.749472|248.074678|381.779|5.077|5.118|

#### 전후 차이의 오차

공통 event-Poisson bootstrap400회(seed202609291). source+input-task+run+lumi/event를 cluster로 묶어 같은 draw를 전후 및 모든 BDT 집단에 공유했다. 후보 중복은 event 단위로 함께 움직인다. 주어진 가중치·source/kinematic 선택에 조건부인 통계오차이며, 전체 생산의 weight 추정·모형 nuisance 불확실성을 포함하지 않는다. ±는 bootstrap SD이며 Gaussian significance로 자동 환산하지 않는다. 95% 구간은 percentile bootstrap 진단 구간이며, boundary에서 정확한 coverage를 입증한 결과가 아니다.

|Family / BDT|ΔFWHM (MeV)|Δwidth68 (MeV)|Δ중앙비율 (%p)|
|---|---:|---:|---:|
|PR/fail|-0.0516 ± 0.2355|+0.1711 ± 0.1369|-0.4464 ± 2.6863|
|PR/pass|-0.6646 ± 0.2351|+0.1451 ± 0.0781|+4.7381 ± 1.8692|
|NPR/fail|-0.5435 ± 0.3211|+0.0538 ± 0.1572|+4.6146 ± 3.0284|
|NPR/pass|-0.2147 ± 0.2585|-0.0939 ± 0.1063|+4.4936 ± 2.1335|

#### BDT와 refit의 결합 효과

I=(after−before)_pass−(after−before)_fail. Pass만의 전후 변화가 양수여도 이것만으로 BDT 특이 효과가 되지는 않는다.

|Family|지표|I ± bootstrap SD|95% percentile 구간|사용 replica|
|---|---|---:|---:|---:|
|PR|DSCB_FWHM_MeV|-0.6130 ± 0.3393|[-0.9576, +0.3457]|381|
|PR|width68_MeV|-0.0259 ± 0.1548|[-0.3871, +0.1981]|400|
|PR|central_fraction_pp|+5.1845 ± 3.2789|[-1.2998, +11.9391]|400|
|PR|KDE_h0p1_FWHM_MeV|-0.1798 ± 0.2404|[-0.6948, +0.2291]|400|
|PR|KDE_h0p2_FWHM_MeV|-0.1094 ± 0.1686|[-0.4478, +0.2402]|400|
|NPR|DSCB_FWHM_MeV|+0.3289 ± 0.4132|[-0.5701, +0.9488]|389|
|NPR|width68_MeV|-0.1478 ± 0.1961|[-0.5561, +0.2431]|400|
|NPR|central_fraction_pp|-0.1210 ± 3.7746|[-6.9705, +7.2006]|400|
|NPR|KDE_h0p1_FWHM_MeV|+0.0368 ± 0.2810|[-0.5203, +0.6010]|400|
|NPR|KDE_h0p2_FWHM_MeV|+0.0642 ± 0.1923|[-0.3309, +0.4150]|400|

모든 위 interaction 구간이0을 포함한다. Prompt-pass 자체의 중앙비율 증가는 +4.738±1.869%p이고, nonprompt-pass도 +4.494±2.134%p 증가한다. Prompt의 pass−fail 차이는 +5.185±3.279%p로 불명확하다. 여러 지표의 탐색적 결과이며 discovery significance로 해석하지 않는다.

#### 기존 그림처럼 inclusive→tight로 비교

|Family / stage|FWHM tight−inclusive (MeV)|width68 tight−inclusive (MeV)|
|---|---:|---:|
|PR/before|+0.0194 ± 0.0770|+0.0056 ± 0.0530|
|PR/after|-0.2013 ± 0.2388|+0.0028 ± 0.0594|
|NPR/before|+0.0581 ± 0.1944|-0.0097 ± 0.0687|
|NPR/after|+0.1094 ± 0.1153|-0.0401 ± 0.0723|

Prompt는 전1.255→1.275MeV, 후0.811→0.610MeV다. 그러나 후의 변화 −0.201±0.239MeV의95% bootstrap 구간은 [−0.691,+0.164]MeV다. Width68은 후1.447→1.450MeV로 거의 동일하다. 따라서 전체 resolution이 tight에서 개선되었다고 결론지을 수 없다.

#### DSCB 모형·수치 민감도

기존과 같은 finite-range weighted unbinned DSCB, 범위140–153MeV, μ145–146MeV, σ0.1–10MeV, αL/R0.1–5, nL/R1–100을 사용했다. ROOT Minuit2 Migrad의 다중 초기값 중 가장 작은 finite NLL을 선택하고 모든 status를 저장했다. Nominal4개 seed, bootstrap은 nominal optimum 및 두 고정 seed를 사용했다. Bootstrap에서 covariance error를 FWHM 오차로 전파하지 않고 refit FWHM 분포 자체를 사용했다.

Nominal12개 fit은 모두 status0. Bootstrap4,800개 중 47개가 nonzero status여서 해당 FWHM contrast에 사용하지 않았다. metrics/contrasts.csv에 valid replica 수가 있다. Nonzero status의 finite FWHM까지 포함한 SE도 별도 열에 기록했고 주 결론은 바뀌지 않는다. 다른 지표는400회 모두 사용한다.

Bootstrap fit 중 2140개가 적어도 한 parameter 경계에 가깝고, 532개가 σ 하한에 도달했다. 특히 prompt-pass-after는195/400개다. 이는 이 표본에서 core/tail 분해가 안정적이라고 주장할 수 없는 근거다. 경계를 확장해 문제를 없앴다고 처리하지 않았다.

DSCB 대신 Gaussian KDE로 shape를 평활화한 prompt-after tight−inclusive FWHM은 h0.1MeV에서 −0.113±0.083MeV, h0.2MeV에서 −0.051±0.064MeV다. KDE도 평활화 의존성이 있으며 detector resolution model이 아니다. DSCB의 큰 점추정만으로 원인을 확정하지 않는다.

직접 DSCB 구현은 기존 StableCrystalBall과15,612개 density 평가에서 상대차0이었다. 독립 Python NLL, 수치 적분, half-maximum endpoints도 검증했다. 원래 pre-refit DSCB 수치도 재현했다. ROOT macro는 interpreter로 실행했으며 ACLiC compilation이나 production 변경은 없다.

#### 저장된 방향 변화와 점수의 연결

다음은 weighted Pearson correlation과 event-bootstrap SD이다. 상관관계는 인과적 원인이나 BDT 오류를 입증하지 않는다.

|Family|x|y|correlation ± SD|
|---|---|---|---:|
|PR|mva|mass_shift_MeV|-0.0131 ± 0.0470|
|PR|mva|abs_mass_shift_MeV|-0.0711 ± 0.0420|
|PR|mva|abs_deta|-0.1338 ± 0.0467|
|PR|mva|abs_dphi_rad|-0.0394 ± 0.0381|
|PR|mva|central_gain_pp|+0.0540 ± 0.0388|
|PR|abs_deta|abs_mass_shift_MeV|+0.4395 ± 0.0597|
|PR|abs_dphi_rad|abs_mass_shift_MeV|+0.5378 ± 0.0689|
|NPR|mva|mass_shift_MeV|+0.0103 ± 0.0384|
|NPR|mva|abs_mass_shift_MeV|+0.0180 ± 0.0339|
|NPR|mva|abs_deta|-0.0583 ± 0.0458|
|NPR|mva|abs_dphi_rad|+0.0185 ± 0.0325|
|NPR|mva|central_gain_pp|-0.0440 ± 0.0425|
|NPR|abs_deta|abs_mass_shift_MeV|+0.3302 ± 0.0557|
|NPR|abs_dphi_rad|abs_mass_shift_MeV|+0.5541 ± 0.0624|

Prompt에서 MVA와 |Δη|는 약−0.134±0.047로, 높은 점수일수록 방향 변화가 항상 더 커진다는 설명과 맞지 않는다. |Δm 이동|은 |Δη| 및 |Δφ|와 상관하지만 MVA와 |Δm 이동|의 상관은−0.071±0.042로 약하다. Score 구간별 평균과 오차는 score_bins.csv에 있다.

#### 판정에 따른 다음 단계

사용자가 정한 조건(동일 source/후보에서 bootstrap과 empirical 폭 지표를 통과)이 충족되지 않았다. 따라서 이번 작업에서는 새 sample 생성, BDT 재훈련, 추가 mass-template/yield-closure 작업을 시작하지 않았다. Yield closure를 통과했다고 주장하지 않으며, 이 결과만으로 nominal mass template의 unbiasedness나 systematic uncertainty를 판정하지 않는다. 먼저 DSCB FWHM의 모형 민감도를 해결해야 한다. 이미 별도로 제출한 posdef replay는 계속 진행하며 그 결과와 이번 저장 후보 검사를 혼합하지 않는다.

선택이 shape를 바꿀 수 있다는 일반적 근거: [Matchev–Shyamsundar, arXiv:1911.12299 초록](https://arxiv.org/abs/1911.12299)은 decorrelation이 “prevents the background distribution from becoming peaked”라고 설명한다. 이는 background-selection에 관한 일반적 논의이며, 이번 signal BDT 또는 vertex refit이 잘못되었다는 증거가 아니다.

#### 그림과 재현

![동일 후보 전후](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/matched_shape_bootstrap/matched_before_after.png)

![같은 source에서 inclusive와 tight 비교](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/matched_shape_bootstrap/matched_loose_tight.png)

Scripts: prepare.py → interpreted bootstrap_fit.C → analyze.py → interpreted validate_pdf.C → plot.py → write_report.py. Numeric outputs: metrics.csv, contrasts.csv, fits.csv, correlations.csv, score_bins.csv. Bootstrap draws and fitted metric replicas are preserved for reproducibility. Both PNG and vector PDF are saved.
<!-- source:SRC-22:end -->

<a id="src-23"></a>

## SRC-23 — bdt_refit_causality_20260929/official_posdef_108/PLAN.md

**기록 구분:** 계획·제안 원문 (완료의 증거 아님). **원문:** [bdt_refit_causality_20260929/official_posdef_108/PLAN.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/official_posdef_108/PLAN.md). **SHA256:** `fd86bc01ba45bf5f72f23a43d6775384cddb64a66d947a27b04e4796f854a6c9`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-23:start -->
### Official positive-definite covariance intervention

Done: reproduce the 108 baseline candidates in their 107 input events; apply the actual CMSSW pseudoPosDefTrack covariance in a study-only unpacker; verify matrix eigenvalues and input-state/metadata preservation; compare frozen-candidate refits to packed and original RECO modes; rerun the complete D0/BDT/D* reconstruction and join by original daughter track identities to measure selection migration and actual BDT-input changes. Save per-candidate outputs, a compact summary, a proposed production diff and report.

Files: PosDefTrackAndVertexUnpacker.cc (new named study module), PosDefCovarianceProbe.cc (new named diagnostic module), replay_cfg.py, replay_worker.sh, prepare_remote.py, analyze_posdef.py, REPORT.md. Existing production sources and previous outputs remain unchanged. Only the existing isolated study runtime gains two plugins and required BuildFile dependencies.

Steps: compile CMSSW plugins with scram b -j 2 (no ROOT macro compilation); run one-event baseline/corrected preflight; validate configuration and outputs; replay the 107 selected events from existing associated.root files; count PSD failures with the audit's normalized-eigenvalue tolerance; compare paired mass, vertex, BDT inputs and scores; document population/target-bin limits. Keep baseline and corrected membership separate. No new event generation or large campaign rerun.
<!-- source:SRC-23:end -->

<a id="src-24"></a>

## SRC-24 — bdt_refit_causality_20260929/official_posdef_108/REFIT_BEFORE_AFTER.md

**기록 구분:** 조사 보고서·해석 (표본·시점별 결과). **원문:** [bdt_refit_causality_20260929/official_posdef_108/REFIT_BEFORE_AFTER.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/official_posdef_108/REFIT_BEFORE_AFTER.md). **SHA256:** `f8ac7ff89986f0e8115b56e6c59d9a2b2d6dea25d871f7ee9ab5e735f84b7338`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-24:start -->
### D* vertex-fit before/after: packed versus original RECO tracks

2026-09-29. Reused all108 baseline candidates (PR54, NPR54) from the completed private pilot; no simulation or reconstruction was launched. This compares each representation to its own pre-fit state, not the difference between two final refit values.

**Confirmed: using original RECO tracks does not remove the slow-pion angular changes or the D* mass-difference changes during refitting.** Their overall sizes remain comparable in this broad sample. A packing-induced covariance defect is real, but the hypothesis that removing that defect makes the before/after changes nearly disappear is not supported by this comparison.

#### Exact definitions

- Before: delta_m_before = M(D0_internal + slow_input) - M(D0_internal). D0_internal is the internally fitted K/pi composite immediately before the D* vertex constraint. slow_input is the input slow-pion momentum at its stored track reference point, with the production pion mass hypothesis (float0.13957018 GeV). It is not yet the refitted slow pion.
- After: delta_m_after = M(Dstar_fit) - M(D0_child_fit).
- Each mode uses its own D0_internal and its own slow input: packed states for packed/official correction/original-covariance-only modes; original RECO states for all_original_tracks.
- The reported mass change is delta_m_after minus delta_m_before. All mass-difference units below are MeV.
- Slow delta_eta and delta_phi compare the input reference-state direction to the final fitted direction; delta_phi is wrapped to [-pi,pi]. These changes include track transport to the fitted vertex and the vertex constraint, not exclusively a covariance-induced correction.

#### All108 candidates

| Metric | Packed | Official posdef | Original RECO tracks |
|---|---:|---:|---:|
| Median absolute delta-m change | 0.1708878 | 0.1708878 | 0.1790822 |
| 90th percentile absolute delta-m change | 0.8130869 | 0.7579775 | 0.9349696 |
| Maximum absolute delta-m change | 3.997197 | 3.997197 | 3.949297 |
| Absolute delta-m change >0.1 MeV (count) | 66 | 65 | 71 |
| Median absolute slow delta_eta | 0.000906726 | 0.0008953165 | 0.0008797665 |
| 90th percentile absolute slow delta_eta | 0.003458592 | 0.003077175 | 0.004122549 |
| Median absolute slow delta_phi (rad) | 0.003185394 | 0.00311034 | 0.003104957 |
| 90th percentile absolute slow delta_phi (rad) | 0.01146854 | 0.01145717 | 0.01154549 |

The >0.1 MeV count is a descriptive threshold, not a fit-failure criterion.61 candidates exceed it with both packed and original tracks;5 cross from above to below,10 from below to above. There is no general suppression of the shift by using original tracks.

#### Prompt and nonprompt separately

| Family | N | Packed median absolute shift | Original median absolute shift | Packed >0.1 count | Original >0.1 count |
|---|---:|---:|---:|---:|---:|
| PR | 54 | 0.165234 | 0.176734 | 32 | 34 |
| NPR | 54 | 0.198943 | 0.202668 | 34 | 37 |

#### Concrete same-candidate example

NPR_118, run929118:lumi1:event651:candidate2. D* pT7.99870 GeV/c, y=-0.304398, hiBin111 (55.5–56% centrality), baseline BDT0.999472. This is outside the target rapidity/centrality bin.

| Track input | Delta-m before | Delta-m after | After minus before | Slow delta_eta | Slow delta_phi (rad) |
|---|---:|---:|---:|---:|---:|
| Packed | 148.613194 | 144.615998 | -3.997197 | 0.0011659 | 0.0265340 |
| Original RECO | 148.607170 | 144.657873 | -3.949297 | 0.0058338 | 0.0281943 |

#### Interpretation and limits

CMS's [Vertex Fitting guide, Track refitting after Vertex fit](https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideVertexFitting#Track_refitting_after_Vertex_fit) explicitly describes re-estimating track parameters “at the fitted vertex position, using this position as a constraint”. Thus a changed direction alone is not proof of an erroneous fit. Assessing whether its size is physically appropriate requires a common-reference-state transport/constraint decomposition and suitable truth residual/pull checks.

Previously observed angular refitting contributes to the conditional D* peak shape. The current numbers show that such angular and mass changes persist with original RECO tracks; they do not establish whether the original-track fit is unbiased or why the specific BDT/cosine distribution differs. The private pilot has no candidates in the original target pT7–10, |y|<0.3, centrality0–10% even before cosine/DCA cuts. This is not a target-bin FWHM measurement.

The earlier25/108 result is a different quantity: official-corrected AFTER minus original-RECO AFTER, exceeding0.1 MeV. The current71/108 is original-RECO AFTER minus its own BEFORE, exceeding0.1 MeV. These counts must not be conflated.

Reproduction: `OPENBLAS_NUM_THREADS=1 python3 compare_refit_before_after.py`. Input: `records.json`. Outputs: `refit_before_after.csv`, `refit_before_after_summary.json`. The production pion mass was verified from DStarFitter.cc lines63–66. A separate rationalized invariant-mass calculation agreed with direct subtraction to better than1e-6 MeV for all540 candidate/mode comparisons; receipt: `refit_before_after_validation.json`. No production source was modified.
<!-- source:SRC-24:end -->

<a id="src-25"></a>

## SRC-25 — bdt_refit_causality_20260929/official_posdef_108/REPORT.md

**기록 구분:** 조사 보고서·해석 (표본·시점별 결과). **원문:** [bdt_refit_causality_20260929/official_posdef_108/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/official_posdef_108/REPORT.md). **SHA256:** `951c5f33f1301566ca37a5163749d302ede26d3465a0edd181b92be22f3ad387`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-25:start -->
### Official CMSSW covariance correction: completed study

Date: 2026-09-29. CERN HTCondor cluster **12747673**, **20/20 completed jobs**. Reused the associated MiniAOD + original RECO products of the completed 2,000-event pilot. Only the **107 events containing the 108 baseline geometrically GEN-matched D* candidates** were reprocessed; no new simulation was generated.

#### Verdict

**Confirmed:** using the actual CMSSW `pseudoPosDefTrack()` covariance removes every observed non-PSD matrix in these candidate tracks. Slow pions: **60/108 -> 0/108**; D0 daughters: **10/216 -> 0/216**. The original daughter track states and all candidate identities are preserved in this comparison.

**Not a full recovery:** after the correction, **25/108** fitted delta-m values still differ from the original-RECO-track refit by more than **0.1 MeV** (27/108 before correction). A numerically positive-definite matrix is not evidence that the original correlations or calibrated uncertainties were recovered. The specific BDT-dependent target-bin FWHM issue is not resolved by this test.

#### Implemented change

Added the study-only `pat::PosDefTrackAndVertexUnpacker` plugin. It copies the original unpacker and changes only the covariance used in the output `reco::Track`, retaining the existing reference point, momentum, charge, chi2/ndof, algorithm, timing, hit pattern, quality and other metadata. The actual official function is called; no Python imitation or custom eigenvalue repair is used in reconstruction.

```diff
 const auto& track = cand.pseudoTrack();
+const auto correctedTrack = cand.pseudoPosDefTrack();
 ...
-                        track.covariance(),
+                        correctedTrack.covariance(),
```

The corrected configuration replaces the module type at the existing `unpackedTracksAndVertices` label, so both the D0 producer (including BDT evaluation) and D* producer consume the corrected collection. The existing branch for candidates without track details is unchanged. This audit's 108 candidates have the detailed covariance information used in the comparison; it does not validate all other track categories.

Original `TrackAndVertexUnpacker.cc` and `D0Fitter.cc` remain byte-identical to their saved baseline hashes (`unchanged_sources.json`). Only the isolated study runtime received the two newly named plugins and plugin build dependencies. No canonical production source was modified. `production_proposal.diff` is the reviewable two-line treatment change, not an applied production patch.

#### Two comparisons

1. Frozen candidates: replay all 108 baseline candidates in seven modes: packed, identity control, slow-only official correction, all-track official correction, slow-only original covariance, all original covariances, all original tracks. All **756/756** candidate/mode fits have valid D0 and D* results. Candidate membership and the baseline daughter mass hypotheses are frozen for these interventions.
2. Full reconstruction: rerun the existing D0, BDT and D* chain with the corrected unpacker, keeping all selections and the model fixed. Match the outputs using run/lumi/event and exact original daughter track identities, independently of candidate index. **108 common, 0 lost, 0 gained** candidates in the 107 selected events. This does not measure gains in the other 1,893 unprocessed pilot events or a campaign-wide efficiency change.

#### Covariance checks

The PSD criterion is minimum eigenvalue of R_ij=C_ij/sqrt(C_ii C_jj) < -1e-10. Diagonal congruence preserves matrix inertia. All matrices are finite, symmetric and have positive diagonal elements.

| Role | Candidate-track instances | Indefinite before | Indefinite after | Minimum normalized eigenvalue after |
|---|---:|---:|---:|---:|
| Slow pion | 108 | 60 | 0 | 0.000438817 |
| D0 daughters | 216 | 10 | 0 | 0.00128085 |

D0 daughters correspond to 214 unique original tracks; the 10 indefinite cases are also 10 unique tracks. These are track counts, not 70 independent problematic D* fits. Original RECO covariances also pass the PSD test, which alone does not validate their statistical coverage.

#### Mass, vertex and BDT results

Delta m is fitted D* mass minus fitted D0 child mass; shifts are corrected minus baseline. These mass-difference units are MeV. D0-first quantities refer to the D0 producer before the additional internal D0/D* refit.

| Full reconstruction quantity | Result |
|---|---:|
| Median absolute delta-m shift | 0.000115122 MeV |
| 90th percentile absolute delta-m shift | 0.00625150 MeV |
| Maximum absolute delta-m shift | 0.674625 MeV |
| Absolute delta-m shift >0.1 MeV | 3/108 |
| Maximum absolute D0-first mass shift | 0.855496 MeV |
| D0-first mass shift >0.1 MeV | 2/108 |
| Maximum D0-first vertex displacement | 9.07039 micrometres |
| Changed BDT scores | 9/108 |
| Maximum absolute BDT-score change | 0.0193592 |
| BDT>=0.95 pass to fail / fail to pass | 0 / 0 |

The 20 actual model-input features are saved for every candidate. Ten candidate rows change their 3D/2D decay-length significance, with maximum absolute changes 0.915869 and 0.834634 respectively. Vertex probability increases by up to 0.0237276. The feature-by-feature table is `bdt_features.csv`. The number of changed scores can be smaller than changed feature vectors for a tree-based model.

All three >0.1 MeV delta-m changes are outside the originally requested pT7–10, |y|<0.3, centrality0–10% target. Values below use the frozen all-track intervention (the full-chain difference for these three is negligible).

| Job; run:lumi:event; candidate | D* pT (GeV/c) | y | hiBin | Delta-m shift (MeV) | Corrected minus original-track refit (MeV) |
|---|---:|---:|---:|---:|---:|
| PR_109; 929109:4:3275; 0 | 8.48870 | -0.34429 | 165 | -0.674625 | 0.671842 |
| NPR_115; 929115:2:1730; 0 | 6.43710 | -0.76692 | 162 | -0.370909 | -0.039785 |
| NPR_116; 929116:3:2353; 8 | 7.40297 | -0.24531 | 34 | -0.256466 | 0.465618 |

#### How close is this to the original RECO refit?

| Comparison to all-original-track refit | Median absolute delta-m difference (MeV) | Absolute difference >0.1 MeV |
|---|---:|---:|
| Packed baseline | 0.04155770 | 27/108 |
| Official correction, full reconstruction | 0.04032711 | 25/108 |
| All original covariance, packed states retained | 0.00610537 | 0/108 |

The official correction fixes the sign problem but leaves most original-track differences in this sample. Even positive-definite packed matrices can differ from original matrices. Restoring the full original covariances has a substantially smaller difference from the original-track reference; remaining state-compression effects are a separate contribution.

The broad-sample median absolute residual to the geometrically matched generator delta m is 0.39799 MeV for packed, 0.39006 MeV for the frozen official correction and 0.34271 MeV for original tracks. These are descriptive numbers in a baseline-selected sample, not a validated resolution improvement, fitted FWHM, significance or uncertainty coverage. The original-RECO fit is a comparison reference, not assumed exact truth.

The earlier 24-candidate official-MiniAOD study found a maximum 0.00216 MeV shift. It was a different deliberately selected cohort and is not a bound for these new 108 candidates; the current maximum is 0.674625 MeV.

#### Verification and limitations

- New CMSSW plugins compiled successfully with `scram b -j 2`; no ROOT macro was compiled. Baseline/corrected one-event preflight completed; 20-job Condor dry-run passed before submission.
- Every worker checked selected event identities and baseline candidate count, exact original-track associations, and successful output creation. All 20 completion markers and validation receipts were read.
- Baseline delta m reproduces the previous pilot to 1.99e-12 MeV maximum. Identity-control delta-m differences are at most 1.07e-11 MeV.
- Full-chain track momentum/reference-state differences are exactly zero before vertex fitting. Applied covariance matrices exactly equal the actual official function's output.
- Full-chain versus frozen all-posdef delta m differs by at most 5.01e-5 MeV; these are separately defined comparisons, and they are not reported as bit-identical.
- The target pT7–10, |y|<0.3, centrality0–10%, tracker |cos(theta*)|0.6–0.8, DCA0–0.08 cm has no candidates already after pT/y/centrality cuts in the pilot. Therefore this audit cannot establish the cause or remedy of that bin's FWHM difference.
- No DATA treatment validation, pull/coverage validation, or campaign-wide selection/efficiency evaluation is claimed. The remaining 25 comparisons are not failed fits or non-PSD matrices: they are >0.1 MeV differences from the original-track reference.

#### Official implementation evidence

[CMSSW_13_2_11 PackedCandidate.cc, lines 155–198](https://github.com/cms-sw/cmssw/blob/CMSSW_13_2_11/DataFormats/PatCandidates/src/PackedCandidate.cc#L155-L198) states “if not positive-definite, alter values to allow for pos-def”. It detects problematic principal minors and, for a negative minimum eigenvalue, raises the diagonal. This is numerical regularization; the implementation does not recover the omitted original correlations.

#### Artifacts and next decision

Primary results: `summary.json`, `candidates.csv`, `bdt_features.csv`, and full `records.json`. Reproduce the aggregation with `OPENBLAS_NUM_THREADS=1 python3 analyze_posdef.py`. The source/worker/config, frozen target list, production proposal diff, build/submission receipts and unchanged-source hashes are saved here. All definitions and denominators are available in `PLAN.md` and the analyzer.

Worker logs, full diagnostic ROOT outputs, expanded configs and paired JSONL: `/afs/cern.ch/user/j/junseok/private/bdt_posdef_108_20260929/jobs/{PR_101..PR_110,NPR_111..NPR_120}/results/`. EOS study mirror: `/eos/home-j/junseok/analysis/dstarana/VertexCompositeStudies/studies/refit_diagnostics_20260918/bdt_refit_causality_20260929/official_posdef_108/`.

The study implementation is complete. The treatment is demonstrated to repair this sample's non-PSD covariance matrices, but cannot yet be promoted as a complete physics remedy. Any production decision must retain this distinction and evaluate the relevant distributions and selection effects in representative DATA and MC. No additional generation or production rollout was performed.


<!-- REFIT_BEFORE_AFTER_20260929 -->
#### Each input representation before versus after the D* fit

Original RECO tracks do not suppress the refit-induced changes in the same108 baseline candidates: median absolute delta-m change packed0.170888 vs RECO0.179082MeV; changes>0.1MeV66 vs71. Median absolute slow-pion delta_eta0.000906726 vs0.000879767; delta_phi0.00318539 vs0.00310496rad. This is each representation compared to its own pre-D* state, not the difference between two final fits. Track transport and vertex constraints are included in these changes. The angular-change/conditional-peak observation does not by itself establish a covariance error as its sole cause. Definitions, matched example and exact table: `REFIT_BEFORE_AFTER.md`.
<!-- source:SRC-25:end -->

<a id="src-26"></a>

## SRC-26 — bdt_refit_causality_20260929/refit_input_interventions/PLAN.md

**기록 구분:** 계획·제안 원문 (완료의 증거 아님). **원문:** [bdt_refit_causality_20260929/refit_input_interventions/PLAN.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/refit_input_interventions/PLAN.md). **SHA256:** `d8d1b3440c968e6e9df79a0fffbb4de65a9ecbe610f6b7da8d1b70f3cd8001c3`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-26:start -->
### Actual fitter input interventions

Goal: isolate whether the BDT-related refit core response arises through D0 position geometry, the relative covariance weighting, or both. Preserve the candidate set, input momenta, BDT scores and event weights. Production is not edited.

Done: reproduce the saved CMSSW D* fit from serialized input states before interpreting any intervention; run explicit geometry/covariance interventions; report paired mass/core response and BDT pass/fail interaction, failures and limitations. A study intervention is not proposed production reconstruction. No GEN reference is required: the recorded reco PV is a coordinate reference for controlled displacement of the D0 line.

1. prepare.py joins geometry records to existing reco-PV records and writes states.json and a provenance receipt. Use the 1229-candidate common set (716PR/513NPR), keeping all initial states and nominal outputs for reproduction checks.
2. Add a distinct study-only CMSSW plugin under RefitAudit/InputIntervention in a fresh minimal CMSSW_13_2_11 runtime under remote /tmp, compile that plugin (not a ROOT macro), run EmptySource for a single framework event. Start with baseline-only reproduction; no MiniAOD reads or new MC are needed.
3. If baseline passes, execute covariance factors0.1/10, separate D0 position/momentum covariance scales and cross-block removal; translate D0 position along azimuthal/other transverse basis components relative to the PV at fixed input momenta. Run a common covariance-scale control. No reclassification/reselection after intervention.
4. Analyze output under this study, event-bootstrap empirical core metrics, compare nominal DSCB fits, save plots/report and verify outputs. Remote commands remain foreground. Do not infer a production treatment from artificial inputs.

Stopping conditions: if baseline cannot reproduce saved output, do not interpret variant physics; diagnose source of mismatch. Record invalid variants explicitly with common-support comparisons, without fallback fits or replacement results.

Execution notes: baseline restoration from the original five-parameter slow-track covariance and reference exactly reproduces all1229 saved fit vertices and daughter momenta. Reconstructing the slow state via Cartesian covariance instead gives a small round-trip discrepancy; it is retained explicitly as state_roundtrip, not used as the baseline. The original strict baseline thresholds were not relaxed. Added both_full_cov0p1/10 controls scaling all seven parameters including mass, and baseline_tight/both_full_cov10_tight with maxDistance1e-5cm and1000iterations, to distinguish covariance and finite-convergence effects. The original both_cov10 scales only the six spatial/momentum coordinates; its mass variance stays fixed and it is not an overall likelihood-scale invariance test.

Final extension:23conditions/28267actual vertex fits. Added uniform relative full7D covariance controls, exact particle/direction/magnitude/mass Shapley decomposition and same-refitted-D0 mass subtraction control. This identifies large artificial covariance broadening as primarily a D0-mass effect, preventing a false claim that the BDT/refit core was cured.552DSCB fits include paired baseline and both mass definitions.
<!-- source:SRC-26:end -->

<a id="src-27"></a>

## SRC-27 — bdt_refit_causality_20260929/refit_input_interventions/REPORT.md

**기록 구분:** 조사 보고서·해석 (표본·시점별 결과). **원문:** [bdt_refit_causality_20260929/refit_input_interventions/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/refit_input_interventions/REPORT.md). **SHA256:** `c503e10ea2654fa4f27c648945e82d1f23d4258e05d635fbbeedd6a30acade4c`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-27:start -->
### BDT 선택과 D* refit: 실제 fitter 입력 개입 결과

#### 결론부터

**확인:** Refit 전 운동량·질량·BDT 점수·가중치를 그대로 둬도, D0 비행선의 위치 관계를 바꾸면 refit 후 Δm의 core가 바뀐다. 특히 D0 운동량에 수직인 방위각 방향 offset을 제거하는 개입의 질량 이동은 slow-pion 방향 변화가 주요 기여다. 따라서 before mass가 BDT 선택 전후 비슷하다는 사실은 D0 위치/기하가 D* refit을 통해 after mass에 영향을 주는 경로와 모순되지 않는다.

**확인:** 같은 prompt BDT통과491후보에서 방위각 offset만 제거하면 nominal FWHM0.610→0.746MeV, 중앙-bin 비율19.791→16.686%다. 중앙-bin 변화−3.105pp의 paired event-bootstrap95%CI는[−5.600,−0.711]pp다. 두 수직 성분을 모두 제거하면0.610→0.901MeV, 중앙-bin16.068%가 된다. 두 성분 조작에는 slow 방향뿐 아니라 refitted D0 질량 변화도 기여한다.

**아직 확정하지 못함:** 이 조작들로 BDT 통과와 전체 후보의 폭 차이가 사라지지 않는다. Pointing 하나가 전체 BDT 의존성을 만들었다거나 covariance 오류가 유일 원인이라고 결론낼 수 없다. 이전 단독-variable 선택 검사는 상관된 D0 pointing/daughter 운동학 선택으로 현상을 재현했고, 이번 검사는 위치 입력을 직접 바꿔 refit 질량 반응을 확인했다. 실제 BDT의 어느 입력 조합이 관측 차이를 얼마나 만들었는지까지는 분리되지 않았다.

**배제한 잘못된 해석:** D0 covariance를10배로 하면 nominal FWHM이 약1.41MeV로 넓어지고 BDT 폭 차이가 작아지지만, 주된 이유는 D0 질량의 이동이다. 동일 refitted D0 질량을 빼는 대조량에서는 좁은 core가 남는다. 따라서 covariance를 임의로 키워서 문제를 해결했다고 주장할 수 없다.

#### 범위와 재현

- Official pThat2/pT4 prompt/nonprompt, D* pT7–10GeV/c, |y|<0.3, centrality0–10%, tracker-axis0.6≤|cosθ*|<0.8, DCA0–0.08cm. BDT>0 baseline, ≥0.95통과.
- 기존 truth_geometry 및 embedded_truth의 공통1229후보: prompt716(통과491/탈락225), nonprompt513(통과351/탈락162),1206events. Prompt는 target 전체; nonprompt는 기존565개 중52개가 빠진 부분 표본이다. 따라서 이번 NPR 값을 이전565개 결과와 같은 값으로 비교하면 안 된다.
- 질량-fit 기준 common140–153MeV에서 prompt715/통과491, nonprompt513/통과351. 각 개입의 invalid·질량창 이탈을 다시 기록하고, 해당 개입과 baseline을 같은 후보에서 비교했다. 새로운 kinematic cut 또는 BDT 재선택은 하지 않았다.
- CMSSW_13_2_11, el8_amd64_gcc11, GlobalTag132X_mcRun3_2023_realistic_HI_v9. 실제 KinematicParticleVertexFitter를 사용한다. 저장된 native D0 state/covariance와 원래 slow track reference/5×5covariance를 복원했다.
- **1229개 모두 baseline vertex, daughter momenta, Δm이 저장 결과와 정확히 동일했다.** 엄격한 검사 허용치를 완화하지 않았다. 초기 Cartesian covariance 왕복 복원은 별도 state_roundtrip 대조군으로 보존했다. 원래 slow track 표현으로 복원해야 exact reproduction을 얻는다.
- 23조건×1229=28267 vertex fits. 기존 MiniAOD 재읽기/새 MC/production 수정 없음. remote/tmp의 별도 minimal CMSSW환경에서 foreground 실행했다. ROOT shape macro는 해석 실행했고 컴파일하지 않았다.

#### 개입의 정확한 의미

D0 입력 운동량 단위벡터 u, reco PV에서 입력 D0 vertex로 향하는 벡터 r에 대해 eφ=(−u_y,u_x,0)/|…|, e⊥=u×eφ를 정의한다. D0 vertex를

`r' = r + (a−1)(r·eφ)eφ + (b−1)(r·e⊥)e⊥`

로 옮긴다. 입력 D0/slow-pion 운동량과 질량 및 covariance는 유지한다. a=0/0.5/1/1.5, b=0/1을 비교했다. 이 조작은 D0 비행선의 옆방향 위치를 바꾸는 **인위적인 원인분리 실험**이다. Fitted D* vertex를 PV에 고정한 것이 아니며, 특히 nonprompt에서 PV를 실제 생성점으로 간주한 처치가 아니다.

Covariance 개입은 C'=SCSᵀ로 정의한다. `full_cov`는7개 성분(x,y,z,px,py,pz,m)을 모두 같은 배율로 확대한다. `d0_cov10` 등 full이 없는 조건은6차원 위치·운동량 부분만 확대하고 mass variance는 유지한다. 두 종류를 섞어 해석하지 않는다. Position-only/momentum-only/cross-block 제거도 실행했다.

공통7차원 covariance×0.1/10에서는 중앙-bin 분류가 바뀌지 않고 prompt 통과FWHM0.61004/0.61020MeV로 유지된다. 상대 전체공분산 대조인 D0×10와slow×0.1은 FWHM1.409898/1.409882MeV로 일치한다. 반대 상대배율도0.641203/0.641242MeV로 일치하나 DSCB sigma 하한에 접한다.

CMSSW 기본 maxDistance0.01cm/100iterations를1e−5cm/1000iterations로 줄인 수렴 대조에서는 prompt 통과FWHM0.606MeV로 좁은 core가 유지된다. 이 조건에서3개 vertex fit이 invalid가 되어 같은 유효후보끼리 비교했다. 드문 후보 이동이 남으므로 모든 후보의 수치 안정성을 증명했다는 뜻은 아니다.

#### Nominal 질량 정의의 결과

N은 질량 fit의 전체/통과 후보 수다. 중앙-bin은145.288135593–145.508474576MeV로 고정했고, core 변화/CI는 valid한 통과 후보의 nominal-weight paired 비교다. FWHM은 점추정이며 새로운 FWHM bootstrap오차를 붙이지 않았다.

|개입|N 전체/통과|전체 FWHM(MeV)|통과 FWHM(MeV)|통과 중앙-bin 변화pp [95%CI]|
|---|---:|---:|---:|---:|
|원래 fit|715/491|0.811|0.610|+0.000 [+0.000, +0.000]|
|두 입자 전체 공분산 ×10|715/491|0.811|0.610|+0.000 [+0.000, +0.000]|
|더 엄격한 수렴|714/490|0.793|0.606|+0.000 [+0.000, +0.000]|
|D0 방위각 방향 offset 제거|715/491|1.070|0.746|-3.105 [-5.600, -0.711]|
|나머지 수직 방향 offset 제거|715/491|1.262|0.707|-2.266 [-4.484, -0.195]|
|두 수직 방향 offset 제거|715/491|1.291|0.901|-3.723 [-6.605, -1.089]|
|D0 전체 공분산 ×10|705/486|1.393|1.410|-6.401 [-10.157, -2.779]|

위 표의 FWHM은 모두status0/bounds0이다. 다만 D0 covariance×10의 비교대상 baseline을 같은486후보로 제한한 fit은sigma 하한에 접한다. 전체별 모든 flagged fit은 validation.json에 있다.

![Actual fitter interventions](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/refit_input_interventions/intervention_fwhm.png)

![Paired central-bin response](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/refit_input_interventions/intervention_core.png)

#### 어느 입자의 무엇이 질량을 움직였나

각 candidate의 실제 전후 fitted four-vector를 섞어 D0/slow-pion 기여를 두 교체 순서에 대해 평균했다. 이어 각 입자의 방향/운동량 크기/질량을 여섯 교체 순서에 대해 평균했다. 이 Shapley 분해의 합은 전체 Δm 변화와1e−7MeV 이내로 일치한다. 상관된 기여의 RMS는 합산할 수 없고 인과비율로 부르면 안 된다.

Prompt 통과491개, 방위각 offset제거:
- 전체 Δm 변화 weightedRMS0.07244MeV.
- Slow 방향 기여RMS0.06810MeV, slow운동량 크기0.00017MeV.
- D0질량 기여0.02352MeV, D0방향0.00053MeV.

따라서 이 위치 개입에서는 **slow-pion 방향 반응**이 질량 이동의 주요 성분이다. 두 수직 성분을 모두 바꾸면 slow방향RMS0.09106MeV와D0질량0.09894MeV가 함께 기여하므로 전부 slow방향 효과라고 부르지 않는다.

D0 전체 covariance×10에서는 전체shiftRMS1.47191MeV, D0질량1.47751MeV, slow방향0.07315MeV다. 이는 주로 D0질량 변화로 nominal Δm가 넓어진 실험이다. 다음 mass-definition control로도 확인했다.

#### Covariance 실험에서 폭이 넓어진 이유를 분리

Nominal Δm은 production에 저장된 기존 D0 mass subtraction을 유지한다. 대조량 `M(D0_fit+slow_fit)−M(D0_fit)`는 subtraction까지 동일 refitted D0 상태로 맞춘다. 이 대조량을 production 처치로 제안한 것은 아니다.

|개입|통과 공통N|대조량 baseline FWHM|대조량 개입 FWHM|개입 status/bound bits|
|---|---:|---:|---:|---:|
|baseline|491|0.605084|0.605084|0/0|
|d0_bend_offset0|491|0.605084|0.712472|0/0|
|d0_transverse_offset0|491|0.605084|0.671154|0/0|
|d0_full_cov10|486|0.591001|0.591431|0/2|

D0covariance×10의 nominal0.591→1.410MeV와 달리, 대조량은0.591001→0.591431MeV로 거의 그대로다. 둘 다sigma하한fit이므로 정확한core폭 추정에는 제한이 있지만, 이 개입의 apparent broadening을 slow 방향의 좁은core 제거라고 설명할 수 없다. Baseline 자체에서는 nominal0.610057와대조량0.605084가 가깝다. 기존 질량정의의 오류를 입증한 결과도 아니다.

Fit에 의존하지 않는491통과후보의 같은-refitted-D0 대조에서도 covariance×10의width68은1.44857→1.45002MeV, 중앙-bin19.791→19.570%로 거의 유지된다. 반면 방위각offset제거는 이대조량에서도 중앙-bin19.791→17.098%로줄어든다. `mass_definition_control.json`에전부저장했다.

#### 왜 before는 비슷한데 after는 달라질 수 있나

현재 D* refit 단계에 입력된 운동량을 p, vertex기하를 V, covariance를 C라 쓰면, before질량은 고정된 p로 정해지고 after질량은 fitter의 F(p,V,C)로 정해진다. BDT는 D0 pointing/비행거리/daughter 운동학 등을 선택한다. Before의 1차원 질량분포가 비슷하다고 해서 (p,V,C)의 결합분포가 같다는 뜻은 아니다. 이번에는 p와score를 고정하고 V만 바꿨을 때 after만 변하는 것을 직접 확인했다.

질량의 방향 의존성은 `M²=m_D0²+m_pi²+2(E_D0 E_pi−|p_D0||p_pi|cosθ)`에 있다. Common-vertex fit이 slow-pion과D0의 상대방향을 바꾸면Δm가 바뀐다. 어느 후보가 피크로 들어오는지는 처음 질량과 보정의 부호/크기에 달려 있으며, 모든 통과후보가 피크로 이동하거나 분해능이 개선된다는 뜻은 아니다.

이전 bdt_selection_response에서는 before 중앙/양옆의 BDT 생존67.29/67.62%와 after73.50/66.68%를 측정했고, pointing 단독선택으로 before폭을 거의 유지하면서 after0.60–0.64MeV를 재현했다. 그 관측과 이번 실제 위치 개입은 **D0 선택과 D* vertex/refit 반응이 연결되는 경로**를 지지한다. 그러나 지금 결과는 전체 BDT 고유효과의 단일 원인/인과분율을 확정하지 않는다.

#### 남은 문제와 다음 검증의 범위

1. 위치 개입 뒤에도 BDT 통과가 전체보다 좁다. 고정 중앙-bin의 pass−fail interaction에 대한 개입 차이도 모든 주된 위치조건에서95%CI가0을 포함한다. 위치가 질량을 움직인다는 paired증거와, BDT에 특이적인 차이를 전부 설명했다는 주장을 구분해야 한다.
2. 정확한20개ONNX입력은 기존 소수24개 replay에만 있다. 나머지 전체후보에서 exact feature-vector/score를 복원해, 상관된 pointing·daughter운동학·오차 입력군의 조건부 선택을 검증해야 유일BDT입력 기여를 더 좁힐 수 있다. 변수별독립shuffling은 비물리적 후보를 만들므로 그 결과를 물리적 원인으로 단정하면 안 된다.
3. 임의covariance배율/위치조작은 error calibration이나physics truth검증이 아니다. 이 결과로 production수정, BDT재훈련, DATA재생산을 시작하지 않았다. 현재출력만으로 reconstruction전체의정당성/오류 또는yieldbias를 판정하지 않는다.
4. 이전 FWHM event-bootstrap에서 통과−전체 변화의CI는0을 포함하고, fittedcore와width68은다르게움직인다. 이번 pairedgeometry개입 효과는 확인했지만 원래 BDT의존성의모집단유의성을 새로확정한것은아니다.

#### 검증, 실패, 파일

- Baseline exact reproduction1229/1229. Inputmomenta/BDT/weights불변 검사,23조건record완전성,후보별동일성 검사통과.
- Vertex invalid: {"baseline_tight": 3, "d0_bend_offset0": 5, "d0_bend_offset1p5": 2, "d0_transverse_offset0": 4}. 합계14mode-candidate failures이며 후보14개라는뜻은아니다. 임의fallback/대체fit없음.
- Shape fits 552개(두질량정의의pairedbaseline포함), status비정상4, parameter-boundary99. 전부명시했고 실패한fit은결론의수치로사용하지않았다. Python 독립수치적분 likelihood 최대차이2.48e-11.
- Bootstrap2000event-Poisson replicas, seed2026092931; family/run/lumi/event단위공통multiplicity,원래weights고정. 탐색적paired진단이고multiplicity보정/독립확증아님.
- `states.json`, `provenance.json`: 복원입력/원본해시. `baseline.jsonl`, `baseline_validation.json`: 재현. `interventions.jsonl`: 실제fitter출력. `candidates.csv/npz`, `summary.csv`, `contrasts.csv`, `shift_decomposition.csv`, `shift_summary.csv`, `fits.csv`, `validation.json`: 상세결과.
- 실행: prepare.py → studypluginbuild/EmptySource실행 → check_baseline.py → analyze.py/decompose.py → interpreted intervention_fit.C → check.py → plot.py → write_report.py. `PLAN.md`, `build_run.sh`, `run_cfg.py`, `BuildFile.xml`, plugin소스를함께보존한다.

#### 공식 구현 근거

CMSSW13_2_11 [KinematicParticleVertexFitter.h의class설명,L8–17](https://github.com/cms-sw/cmssw/blob/CMSSW_13_2_11/RecoVertex/KinematicFit/interface/KinematicParticleVertexFitter.h#L8-L17)은 “refit the daughter particles with the knowledge of vertex”라고 설명한다. [실제fit구현,L48–85](https://github.com/cms-sw/cmssw/blob/CMSSW_13_2_11/RecoVertex/KinematicFit/src/KinematicParticleVertexFitter.cc#L48-L85)은 inputstate에서linearization point를 구하고 commonvertex를fit한뒤daughterstate를만든다. 이공식설명은위치제약이운동량재추정에들어간다는근거이며,이번BDT원인에대한증거는위의동일후보개입결과다.

KinematicState의 [FTS생성자와parameter생성자](https://github.com/cms-sw/cmssw/blob/CMSSW_13_2_11/RecoVertex/KinematicFitPrimitives/interface/KinematicState.h#L28-L42), [Cartesian↔curvilinear변환](https://github.com/cms-sw/cmssw/blob/CMSSW_13_2_11/TrackingTools/TrajectoryState/src/FreeTrajectoryState.cc#L27-L39)을확인해baseline복원표현을맞췄다. 원본slowtrack복원전의미세한round-trip차이를원래productionfit불안정으로오인하지않았다.
<!-- source:SRC-27:end -->

<a id="src-28"></a>

## SRC-28 — bdt_refit_causality_20260929/refit_mass_decision/PLAN.md

**기록 구분:** 계획·제안 원문 (완료의 증거 아님). **원문:** [bdt_refit_causality_20260929/refit_mass_decision/PLAN.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/refit_mass_decision/PLAN.md). **SHA256:** `83974b6ef60a5b38c24b8a29116aea3574436cc0c8f54b7bd746ae3bf818ee56`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-28:start -->
### Refit mass decision: frozen-candidate truth and yield closure

Goal: determine whether additional D* refit improves mass accuracy and whether the distinct PR/NPR shapes bias the existing mass-to-DCA extraction. Scope: official pThat2/pT4, pT7-10, |y|<0.3, centrality0-10%, tracker cosine five folded bins, DCA0-0.08cm. No production edits, model retraining, new MC generation, or claimed full reconstruction replacement.

Done: exact candidate/GEN association; paired truth bias/width/tail and mass FWHM by cosine and BDT selection; signal-injected background-plus-PR/NPR mass fits with inclusive shape calibration and fixed-shape DCA slices; prompt-fraction recovery; same before/after toy identities; positive closure control, explicit failure/precision limits; plots and report with a physics decision. If data lack overlap or estimator fails, report that result rather than invent a replacement.

1. Recover GEN 4-vectors from preserved diagnostics (four-reader foreground extraction, checkpoint receipts). Check mother-chain and identity. Existing recovered four candidates use their preserved remote diagnostic ROOT.
2. Compute event-bootstrap truth metrics across five cosine bins, preserving all nominal cuts/weights and reporting window conditioning. Reuse the interpreted DSCB shape fitter; distinguish truth residual widths from mass FWHM.
3. Freeze a bounded closure design after inspecting actual production PDF/minimizer, nine DCA bins, and current DATA normalizations. Use actual production headers/functions from study wrappers. Compare empirical held-out MC truth against pooled MC supplier tails, DATA inclusive mean/width calibration, fixed slice shapes, and DCA extraction. Include model-matched positive controls and independent split directions. No common-shape generator as the sole test.
4. Run a small timing/reproduction preflight before a finite toy batch. Record fit failures, MC support, finite-template limits and uncertainty method. Do not reinterpret diagnostic differences as automatic systematics.
5. Verify likelihood/injection identities, inspect PNG/PDF, update parent REPORT/STATUS and archive small artifacts to existing lxplus study.

Files: this directory only plus parent report/status summaries. Commands: python3/uproot, existing scipy/ML venv if needed; ROOT interpreted via thisroot.sh, never ACLiC. Read-only existing production source and data. Large expansion/new production requires a separate decision.

Completed: 6159 exact GEN links; 2000 event bootstrap; 60 DSCB fits (one nonzero status reported); 44 paired toys / 880 numerical-pass mass fits; 26664 DCA scans, including matched-error oracle; four PNG/PDF pairs. Limits: frozen selection, official-only MC, two fixed folds, conditional toy errors, missing DCA inflow for scale<1, no full-chain covariance/coverage or new production.
<!-- source:SRC-28:end -->

<a id="src-29"></a>

## SRC-29 — bdt_refit_causality_20260929/refit_mass_decision/REPORT.md

**기록 구분:** 조사 보고서·해석 (표본·시점별 결과). **원문:** [bdt_refit_causality_20260929/refit_mass_decision/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/refit_mass_decision/REPORT.md). **SHA256:** `f5bb97c77895ab613b49061a7f44dab7470e97cea493622ea807d71de0e7e9d8`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-29:start -->
### D* 추가 vertex refit 판단: 동일 후보 truth 및 mass→DCA closure

#### 물리적 결론

**현재 추가 D* refit의 좁은 prompt FWHM을 질량 정확도 개선으로 정당화할 수 없다. 하지만 refit을 생략하는 것만으로 현 mass→DCA yield 추출이 검증되는 것도 아니다.**

1. **확인:** 문제의 prompt 통과 후보 491개에서 fitted FWHM은 1.275→0.610 MeV이나, GEN 질량차에 대한 68% 절대오차 반경은 0.674→0.773 MeV이다. 증가 +0.099 MeV의 paired event-bootstrap 95% 구간은 [+0.014,+0.154] MeV로 0보다 크다. 피크 core와 대부분 후보의 정확도는 다른 지표다.
2. **확인:** prompt 통과 후보의 GEN-residual width68과 68% 절대오차 반경은 다섯 cosine 빈 모두 중앙값 기준으로 증가한다. 모두 유의하다는 뜻은 아니다. Width68의 pointwise 95% 구간이 0을 배제하는 것은 0.8–1.0이고, 절대오차 반경은 0.6–0.8 및 0.8–1.0이다. Nonprompt에는 이 일관된 증가가 없다.
3. **확인:** 보존한 MC를 겹치지 않는 두 event 집단으로 나누고 supplier/검증 역할을 바꾸면, 공통 DSCB shape를 쓰는 질량 추출의 yield 편향은 refit 전후 모두 남고 두 분할에서 부호도 바뀐다. 독립 MC shape/유한 표본 한계가 커서 한 방향의 재구성 편향값으로 합치면 안 된다.
4. **판단:** PR/NPR FWHM 차이 자체를 없애는 것이 목표가 아니다. 그 차이와 DCA별 구성 변화가 yield와 prompt fraction에 만드는 편향을 통제해야 한다. 우선 비교 대상으로 추가 D* refit 전 질량을 유지하되, 공통 shape 가정도 함께 검증해야 한다. 본 결과만으로 DATA 전체 재생산, BDT 재훈련 또는 production 변경을 결정하지 않는다.

이전 실제 fitter 입력 개입/BDT 연구가 지지한 것은 **D0 기하 선택과 refit 반응의 결합**이다. 이번 검사는 그 결합의 결과를 정확도·추출량으로 평가한 것이며, 유일한 BDT 입력이나 covariance 결함을 새로 확정한 것은 아니다.

#### 범위와 동일 후보 조건

- Official pThat2/pT4 PR/NPR만 사용. pT 7–10 GeV/c, |y|<0.3, centrality 0–10%, tracker 축 folded |cosθ*| 5개 빈, DCA 0–0.08 cm. 이 자료는 전체 private+official production 혼합 MC를 대표하지 않는다.
- 원래 nominal 선택을 통과한 6,159개 후보의 index/event/track key/BDT 점수와 nominal 질량을 확인하고 GEN D*→D0+slow-pion mother chain까지 전수 연결했다. 실패·누락 0. `truth_records.json`에 원본 파일과 연결 근거를 보존했다.
- Before는 `deltaMOriginal`, after는 실제 nominal `mass−massDaugther1`이다. 별도 diagnostic `deltaMRefit`와 혼동하지 않는다. Before는 **추가 D* vertex fit 이전**이며 D0 자체의 reconstruction/BDT를 제거한 것이 아니다.
- GEN residual은 각 후보의 저장된 GEN 4-vector로 계산한 Δm_GEN을 뺀다. PDG 상수/GEN vertex 위치를 대용하지 않는다. GEN Δm 범위는 145.033598–146.637678 MeV이며, 4-vector closure 최대차이는 2.03e-06 GeV이다.
- 기본 truth 결과(`frozen`)는 원래 nominal 후보를 고정한 조건부 비교다. Refit을 없앤 전체 selection/efficiency를 재계산한 것은 아니다. DSCB/closure에는 before와 after가 모두 140–153 MeV인 공통 6,151개를 쓴다. 공통-window truth 결과도 별도 제공한다.
- 각 후보의 원래 weight를 고정하고 event 단위 Poisson bootstrap 2,000회로 전후 상관을 보존했다. Weight 추정 자체의 오차는 포함하지 않는다. MVA pass≥0.95, fail은 0<MVA<0.95.

#### GEN 기준 결과

Width68은 residual의 q84−q16이며 **반폭이 아니다**. 절대오차68은 P(|Δm_reco−Δm_GEN|≤r)=0.68인 반경이다. 단위는 MeV/c². 아래 CI는 전후 차이의 pointwise 95% percentile 구간이며 다중검정 보정은 하지 않았다. FWHM은 별도 DSCB 중앙값이며 bootstrap 오차가 0이라는 뜻이 아니다.

|종류|cos 빈|N / N_eff|FWHM 전→후|GEN width68 전→후|절대오차68 전→후|절대오차68 변화 [95% CI]|
|---|---|---:|---:|---:|---:|---:|
|Prompt|0.0–0.2|445 / 443.3|1.126→0.920|1.284→1.452|0.638→0.670|+0.033 [-0.041, +0.123]|
|Prompt|0.2–0.4|449 / 443.7|1.148→1.023|1.196→1.301|0.589→0.648|+0.060 [-0.010, +0.101]|
|Prompt|0.4–0.6|416 / 413.4|1.218→1.031|1.289→1.363|0.648→0.725|+0.077 [-0.039, +0.143]|
|Prompt|0.6–0.8|491 / 485.8|1.275→0.610|1.305→1.450|0.674→0.773|+0.099 [+0.014, +0.154]|
|Prompt|0.8–1.0|433 / 424.4|1.035→0.771|1.290→1.546|0.601→0.745|+0.144 [+0.044, +0.216]|
|Nonprompt|0.0–0.2|383 / 381.4|1.492→1.214|1.434→1.305|0.701→0.733|+0.032 [-0.076, +0.095]|
|Nonprompt|0.2–0.4|423 / 412.1|1.351→1.069|1.357→1.257|0.690→0.642|-0.048 [-0.128, +0.016]|
|Nonprompt|0.4–0.6|393 / 384.4|1.298→1.196|1.262→1.366|0.649→0.687|+0.038 [-0.034, +0.132]|
|Nonprompt|0.6–0.8|388 / 381.8|1.249→1.034|1.421→1.319|0.691→0.662|-0.030 [-0.145, +0.090]|
|Nonprompt|0.8–1.0|389 / 378.9|1.559→0.947|1.520→1.415|0.770→0.664|-0.106 [-0.199, -0.014]|

문제 빈 prompt의 평균 residual은 +0.203→+0.232 MeV이며 그 변화 CI는 0을 포함한다. 따라서 여기서 확인한 68% 절대오차 증가를 평균 bias의 유의한 증가와 동일시하지 않는다. Nonprompt 문제 빈의 width68은 1.421→1.319 MeV이나 변화 CI는 0을 포함한다. **Nonprompt가 전체적으로 올바르게 refit된다고 확정한 결과도 아니다.**

![GEN width](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/refit_mass_decision/truth_width_all_cos.png)
![Absolute error and tails](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/refit_mass_decision/truth_accuracy_changes.png)
![FWHM](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/refit_mass_decision/fwhm_all_cos.png)

#### Closure 설계: 무엇을 실제 production에서 재사용했나

- `closure.C`는 실제 production `FitSingleBinPDFFactory.h`, `FitSingleBinMinimization.h`와 `FitDCATemplateFromSingleBinOutputs.cpp`를 include하여 ROOT에서 해석 실행했다. Production 파일 수정/컴파일 없음.
- 공통 tight 대상 PR491/NPR388을 event hash로 A/B 분할했다. Supplier와 DCA template은 training fold, empirical pseudo-data는 반대 fold에서 원래 상대 가중치로 추출한다. 양쪽 역할을 바꾼다. 각 fold를 여러 번 추출해도 독립 MC 통계가 늘어난 것은 아니다.
- 각 toy의 PR/NPR 수를 Poisson 생성하고 before/after는 **같은 후보, 같은 DCA, 같은 background realization**을 사용한다. Injected f_prompt는 0.8와 0.5이다. 검증 표본과 supplier의 PR/NPR 혼합도 실제 pooled supplier 규칙대로 다를 수 있다.
- 기존 Sep25 DATA의 해당 빈 14,620 signal 및 약 194,412 background를 exposure 기준으로만 사용했다. 9개 DCA별 threshold-exponential 배경 계수도 여기서 읽었다. 오래된 MC를 새 campaign MC로 대체한 것이 아니다.
- Weighted MC supplier의 DSCB tails를 DATA inclusive fit에 넘겨 mean/width를 재추정하고, 6개 signal parameter를 9개 DCA 질량 fit에 고정했다. Threshold-exponential background와 nsig/nbkg는 각 slice에서 자유다. Multistart와 weighted AsymptoticError는 production 함수가 수행한다.
- 빠른 ensemble은 DATA exposure 10%, empirical 8회 + common-DSCB positive control 2회 × 2 folds × 2 fractions이다. 추가로 실제 DATA exposure의 empirical/control 각 1회 × 2 folds(f_prompt=0.8)를 계산했다. 후자는 고통계 spot check이며 ensemble/coverage 측정이 아니다.
- Positive control에서는 signal 질량만 공급 DSCB에서 생성해 family/DCA와 독립으로 만들었다. DCA와 background를 동일하게 두어 signal shape 가정이 맞는 경우의 대조군으로 쓴다. 정확히 같은 PDF를 생성·fit하는 대조군만으로 실제 MC를 검증하지 않았다.
- DCA fit은 먼저 scale=1에서 actual `PerformDCATemplateFit`을 사용했다. 정확한 signal counts를 DCA fit한 oracle과 mass-extracted yields의 DCA fit을 비교하여 mass 단계의 추가 영향을 분리했다. Oracle도 독립 MC DCA template 때문에 true f와 다를 수 있다. 아래 결과에서는 같은 mass-yield 오차를 적용한 oracle를 추가하여 central yield 이동과 오차 가중 변화를 분리했다.
- 전체 macro의 모든 retry/profile/MINOS/오차 전파 정책을 bit-for-bit 재현한 것은 아니다. 이 study는 unbinned PDF·최소화·shape handoff·DCA fit을 직접 쓰지만 HESSE와 제한된 seed를 사용한다. Inclusive calibration과 DCA slice 간 covariance까지 전파한 coverage 검증은 아니다.

#### Closure 결과

**오차 가중 효과 분리:** Poisson 오차를 쓴 정답 oracle와 mass fit 오차를 쓴 정답 oracle를 모두 계산했다. 독립 DCA template가 진실과 정확히 같지 않으면 오차 가중만 바뀌어도 f가 달라질 수 있다. 표는 같은 오차 oracle 기준이며, 원래 Poisson oracle 대비 이동과 그 분해도 CSV에 보존한다.

아래 yield bias=(회수−주입)/주입이며 %, fraction shift는 **추출 f−같은 mass-yield 오차를 사용한 정답 oracle f**로 pp이다. ±는 고정 MC fold에 조건부인 toy 평균의 표준오차다. 반복 toy를 독립 MC template 통계로 해석하면 안 된다.

|Fold|주입 f|조건|N toys|yield bias 전→후 (%)|yield 변화에 의한 fraction shift 전→후 (pp)|
|---|---:|---|---:|---:|---:|
|A|0.8|empirical|8|-11.10±2.51 → -9.60±2.34|+0.36±1.43 → -2.65±1.92|
|A|0.8|model control|2|+5.82±9.07 → +4.70±13.39|+2.66±0.61 → +4.95±1.10|
|A|0.5|empirical|8|-15.01±2.74 → -6.07±3.93|+5.28±1.44 → +1.38±1.30|
|A|0.5|model control|2|+1.23±1.53 → -0.51±0.03|+2.15±1.13 → +1.85±1.20|
|B|0.8|empirical|8|+16.13±2.28 → +22.09±5.58|-2.62±1.38 → -3.53±1.48|
|B|0.8|model control|2|-4.05±2.04 → -1.30±1.56|-4.00±4.89 → -3.47±4.91|
|B|0.5|empirical|8|+16.08±2.48 → +30.40±4.22|+3.65±1.84 → -0.30±2.05|
|B|0.5|model control|2|+3.24±11.88 → +3.76±12.47|+0.60±0.24 → -0.62±1.48|

동일 toy 전후 차이의 CI는 `closure_paired_changes.csv`에 있다. 전체 f bias와 oracle 자체의 template bias는 `closure_group_summary.csv` 및 `closure_all.csv`에 분리했다. 수치적 fit 성공은 yield closure 성공과 다르다.

![Closure](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/refit_mass_decision/closure_comparison.png)

##### 실제 DATA exposure spot check

|Fold|조건|주입 Nsig|회수 Nsig 전→후|yield bias 전→후 (%)|f−matched oracle 전→후 (pp)|
|---|---|---:|---:|---:|---:|
|A|empirical|14688|12520.1 → 12979.3|-14.76 → -11.63|+0.31 → -2.34|
|A|model control|14688|14527.5 → 14439.2|-1.09 → -1.69|+0.20 → -0.01|
|B|empirical|14705|17065.2 → 18481.7|+16.05 → +25.68|+1.22 → +0.11|
|B|model control|14705|14934.9 → 15464.3|+1.56 → +5.16|+1.94 → +1.62|

##### DCA scale 재선택

Production과 같은 0.60–1.60, 0.01 간격을 동일 DCA fit 함수로 평가했다. **범위 제약:** 원래 저장 후보는 DCA<0.08 cm라 scale<1에서 유입될 DCA≥0.08 cm 후보가 없다. 전체 grid는 frozen-sample 진단이며 nominal acceptance scan의 완전한 재현이 아니다. 유입 누락이 없는 scale1.00–1.60도 비교했지만 이는 넓히는 방향만 시험한다.

|DATA exposure / empirical|grid|best scale 전→후|f−matched oracle 전→후 (pp)|
|---|---|---:|---:|
|A|frozen_060_160|1.20 → 1.20|+1.14 → -2.33|
|A|no_inflow_100_160|1.20 → 1.20|+1.14 → -2.33|
|B|frozen_060_160|0.92 → 0.83|+10.52 → +0.70|
|B|no_inflow_100_160|1.00 → 1.00|+1.22 → +0.11|

같은 오차를 적용한 oracle도 같은 grid에서 독립적으로 scale을 선택했다. 따라서 이 표의 차이는 mass yield 변화가 scale 선택을 바꾸는 효과까지 포함한다. 전체 toy/scale/경계 기록은 `dca_scan*.csv`에 있다. Scale 재선택은 질량 단계에서 이미 발생한 signal-yield 편향을 없애지 않는다.

#### 검증과 한계

- 880 DATA mass fits / 88 stage chains / 44 paired toys. Mass numerical nonpass 0, status 비정상 0, covQual≠3 0. 정확한 전달/주입 counts 및 전후 oracle 항등식 검증 통과.
- DCA scale1 해석적 최소점과 ROOT의 f 차이 최대 0.000258, χ² 차이 최대 2.26e-05. DCA status 집계: `{"('0', '0', '3')": 132}`.
- 별도 DSCB shape fit 60개 중 status 비정상 1, boundary 접촉 23개. 비정상은 PR cos0.2–0.4 / MVA>0 / before의 status3(EDM≈1.1e−6)이고 tight 결과에는 없다. 독립 Python likelihood·정규화·FWHM 식 검증 통과(NLL 최대차이 2.56e-11). 경계나 낮은 EDM을 완전한 shape 불확실성 검증으로 해석하지 않는다.
- DCA scale scan 26,664개는 모두 migrad/status0, covQual3이었다. Matched-error oracle의 ROOT와 독립 해석식 f 차이는 최대 9.72e−5이다. 수치 성공과 full acceptance 재현은 별개다.
- Positive control도 각 조건 2회 및 고통계 1회뿐이라 완전한 closure/coverage 인증이 아니다. 예를 들어 고통계 B after control의 inclusive yield 초과는 해당 HESSE 오차 약 1.4배이고, 이 한 번의 값으로 편향을 확정하지 않는다.
- Prompt/nonprompt와 DCA 사이 상관, pooled supplier 혼합, 고정 DSCB의 모형 오차, 작은 MC 표본을 이 시험 하나로 분리할 수 없다. 특히 fold 부호 변화는 단순한 refit-only 편향값으로 사용하기 어렵다는 증거다.
- 생성한 배경을 전후 동일하게 유지했다. 실제 DATA의 no-refit background, cos/DCA/BDT 재선택, reconstruction efficiency 변화는 시험하지 않았다. 현재 no-refit 후보는 after selection에 조건부이므로 전체 no-refit reconstruction 검증이 아니다.
- 고정 shape calibration에 따른 bin 간 covariance, MC event cluster/weights 추정의 full-chain covariance, 불확실성 coverage, 최종 ρ00 bias는 미검증이다. 이번 차이를 곧바로 systematic uncertainty로 책정하지 않는다.

#### 다음 조치의 판단 기준

**Production 변경 전 필요한 것은 PR/NPR 폭을 강제로 같게 만드는 처치가 아니라, 실제 서로 다른 질량모양을 허용했을 때의 추출 closure다.** 후보 방안은 mass×DCA 동시모형 또는 DCA별 조성에 대응하는 질량 shape이며, 예를 들어 각 성분에 S_PR(Δm|DCA)T_PR(DCA), S_NPR(Δm|DCA)T_NPR(DCA)를 별도로 둔다. PR/NPR 질량모양만 분리하고 성분 내부 DCA 의존성을 무시해도 충분한지는 추가 검증 대상이다.

추가 D* refit 전 질량은 prompt truth 정확도 면에서 우선 비교할 후보지만, 동일한 closure와 DATA sideband 검증을 통과해야 채택할 수 있다. 충분한 독립 MC support를 확보하고 source-family를 유지한 분할검증이 필요하다. 현재 결과는 새 MC 생성/BDT 재훈련/DATA 전체 재처리를 곧바로 요구하지 않는다. 큰 production은 실행하지 않았다.

#### 방법 근거와 재현

CMS Combine 공식 설명의 [Statistical Tests](https://cms-analysis.github.io/HiggsAnalysis-CombinedLimit/latest/what_combine_does/introduction/#statistical-tests)는 “Generation of pseudo-data from the model”을 명시한다. 여기서는 그 생성→fit 검증 원리를 사용했다. Combine 자체를 실행한 것은 아니며, 이 일반 방법 문서가 이번 D* reconstruction의 타당성을 보증하는 것은 아니다.

주요 명령 순서: `prepare.py` → `extract_truth.py` → `analyze_truth.py`; ROOT interpreted `feature_fit.C`; `run_closure.py`; `analyze_closure.py`; ROOT interpreted `scan_dca.C`를 ensemble/full_exposure에 각각 실행 → CSV 병합 → `analyze_scan.py`; `validate_shapes.py`, `plot_truth.py`, `plot_closure.py`, `write_report.py`. ROOT 환경은 `/software/ROOT/ROOT-v6.24/root-6.24-install/bin/thisroot.sh`. 모든 작업은 foreground 감독 실행이며 production source는 수정하지 않았다.

원본/source 목록 및 SHA256은 `provenance.json`, 완료 상태는 각 `runs/*/receipt.json`, 수치 검증은 `*_validation.json`을 참조한다. PNG와 PDF 모두 저장했다.
<!-- source:SRC-29:end -->

<a id="src-30"></a>

## SRC-30 — bdt_refit_causality_20260929/target_bin_posdef/PLAN.md

**기록 구분:** 계획·제안 원문 (완료의 증거 아님). **원문:** [bdt_refit_causality_20260929/target_bin_posdef/PLAN.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/target_bin_posdef/PLAN.md). **SHA256:** `f619544542bc59fadf94dc118be7680a1fa81afaedda9a753eeb3a470100137d`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-30:start -->
### Posdef replay of the actual affected bin

Done: replay the 1,258 existing official PR/NPR pT4 MiniAOD events containing 1,281 previously selected candidates; validate baseline identity and mass/BDT reproduction, then compare matched baseline/posdef mass distributions, slow-pion directions, BDT and losses. Preserve candidate weights and original-bin membership. Do not claim incoming-bin acceptance or recovery of original RECO covariance.

Scope: D* pT 7–10 GeV, |y|<0.3, centrality 0–10%, tracker |cos(theta*)| 0.6–0.8, DCA 0–0.08 cm, original BDT>=0. Separate original BDT>=0.95 and reselected BDT. Official PR pT4 716 and NPR pT4 565; private MC and other source thresholds excluded.

1. Build targets.json and jobs.json from the verified candidate manifest/paired arrays, retaining LFN/event/three daughter identities, weights and expected states.
2. Use replay_cfg.py with the preserved official production configuration and already built study PosDefTrackAndVertexUnpacker; no production edits or compilation.
3. Run baseline and corrected for one input file; read ROOT output with uproot and verify expected baseline candidates before batch submission.
4. If this succeeds, submit bounded file-group jobs through the existing CERN Condor workflow; save queue, exit and output receipts. No new MC generation.
5. Match corrected candidates by input LFN/run/lumi/event and daughter keys, report missing/ambiguous matches explicitly, and compare full distributions without declaring a fixed 0.1 MeV shift a failure.

Commands: python3 prepare_targets.py; isolated cmsRun replay_cfg.py; uproot extraction/validation; condor_submit -dry-run followed by CERN submission after preflight. Stop dependent production if baseline reproduction fails. All intentional scripts/results remain in this campaign study; remove incidental test artifacts.
<!-- source:SRC-30:end -->

<a id="src-31"></a>

## SRC-31 — bdt_refit_causality_20260929/target_bin_posdef/REPORT.md

**기록 구분:** 조사 보고서·해석 (표본·시점별 결과). **원문:** [bdt_refit_causality_20260929/target_bin_posdef/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/target_bin_posdef/REPORT.md). **SHA256:** `c0da36fbad3e8ba28a53cf809ff3c67feb792daa77803f28fbed7e306519527b`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-31:start -->
### Actual target-bin posdef replay

Submitted CERN Condor cluster **12755181**: 64 jobs, at most 16 materialized. 1,258 existing events in 877 official MiniAOD files; 1,281 frozen candidates (PR716, NPR565). These are the actual problem-bin candidates, not the broad private pilot. Scope is official pT4 sources only.

Selection: D* pT7–10 GeV, |y|<0.3, centrality0–10%, tracker |cos(theta*)|0.6–0.8, DCA0–0.08cm, BDT>=0. Original candidate membership/weights are retained. Compare original BDT>=0 and >=0.95; any new BDT migration is reported separately. Incoming candidates from events outside this frozen sample are not measured.

Both baseline and corrected reconstruct D0 and D* from the same MiniAOD. The sole physics intervention is the already built study PosDefTrackAndVertexUnpacker using the official pseudoPosDefTrack covariance while preserving the input momentum and track metadata. Canonical production code and existing outputs are unchanged. No new MC generation or plugin build is involved.

Preflight: one real target event, baseline exit0 (21.11s), corrected exit0 (24.23s). The baseline mass, diagnostic Delta-m, BDT and slow eta/phi match prior production; both treatments retain the identified candidate. This validates the replay mechanism; it is not a population-level physics result.

Results pending. Submission is not completion. extract_results.py validates every available baseline against the frozen candidates and matches corrected candidates using LFN/run/lumi/event and K/pi/slow keys. Missing or ambiguous identities are not silently replaced. Comparison uses full Delta-m and angular distributions; 0.1MeV is not a failure criterion.

Output: remote target_bin_posdef/jobs/{PR,NPR}_xx/{baseline,corrected}.root plus logs and framework reports. ROOT outputs go to EOS, not the nearly full AFS home. AFS stores the submit bundle and small job receipts. Source hashes and exact submission paths are in preparation.json and submission.json.
<!-- source:SRC-31:end -->

<a id="src-32"></a>

## SRC-32 — bdt_refit_causality_20260929/truth_geometry/PLAN.md

**기록 구분:** 계획·제안 원문 (완료의 증거 아님). **원문:** [bdt_refit_causality_20260929/truth_geometry/PLAN.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/truth_geometry/PLAN.md). **SHA256:** `9bf02612313b09721e710015f8bfcbf335e0e475d6c109c7ba1a5e97a1400396`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-32:start -->
### Same-candidate truth and vertex geometry diagnosis

Done: link the actual official pT4 target candidates (PR716/NPR565, 1,258 events) to decay-chain-consistent GEN D*/D0/slow-pion truth; compare mass residual bias/width68/tails and angular changes by original BDT pass/fail; split propagation from vertex-constraint changes at a common transverse-PCA reference; condition comparisons on overlapping D0/slow kinematics. Save fit states, covariances, vertices/chi2, validity flags, exact joins, plots and conclusions/limitations.

Scope remains pT7–10, |y|<0.3, centrality0–10%, tracker |cos|0.6–0.8, DCA0–0.08cm. Original event/candidate membership and physical weights are frozen. No new MC, BDT training, bootstrap, or canonical production modifications. Do not stop existing posdef cluster12755181.

1. Extract GEN, ancestry, state/covariance and vertex fields already present in completed target_bin_posdef ROOT outputs, with baseline reproduction checks. Reuse outputs, not repeated production of existing information.
2. Add only CommonVertexProbe.cc to the isolated study CMSSW plugin package. Replay the same MiniAOD events without changing reconstruction. Save slow/D0 input KinematicStates, states propagated to the final D* vertex transverse PCA, fitted states, and slow input/fitted propagation to the GEN production vertex. StateAtPoint means transverse PCA relative to the reference point; the unfit trajectory is not forced through that point.
3. One-event replay must reproduce production states before a bounded CERN Condor submission. Use existing immutable runtime bundle plus this study overlay; baseline-only, no repeated posdef intervention. Record build/source hashes and output receipts. Build CMSSW plugin only; never compile ROOT macros.
4. Analyze natural D0 flight geometry, transverse separation of propagated trajectories, input covariance validity, vertex chi2 and state response. Do not label (fit-input)/input error a pull. No formal change-pull without validated input/output cross-covariance. Distinguish GEN residuals from independent hit-level truth association, which MiniAOD does not provide here.
5. Compare fixed-bin PR/NPR pass/fail and common-support kinematic groups. Correlation/overlap adjustment is diagnostic, not causal proof. Report missing/invalid/ambiguous cases; do not invent recovered original RECO covariance.

Commands/files: extract_stored.py; prepare.py; CommonVertexProbe.cc; replay_cfg.py; worker.sh; prepare_remote.py; scram b -j2 in isolated runtime; preflight cmsRun; condor_submit -dry-run and CERN submission; analyze.py/plot.py/REPORT.md. Stop dependent analysis if identities or independent-fit reproduction fail. Inspect generated artifact paths and figures. Retain intentional scientific receipts; remove arbitrary execution scratch.

Official reference: CMS SWGuideVertexFitTrackRefit Introduction says parameters are re-estimated at the fitted vertex, while hit measurements remain those from the original track fit. CMSSW13_2_11 TransientTrackKinematicParticle::stateAtPoint explicitly uses propagateToTheTransversePCA.
<!-- source:SRC-32:end -->

<a id="src-33"></a>

## SRC-33 — bdt_refit_causality_20260929/truth_geometry/REPORT.md

**기록 구분:** 조사 보고서·해석 (표본·시점별 결과). **원문:** [bdt_refit_causality_20260929/truth_geometry/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/truth_geometry/REPORT.md). **SHA256:** `1677012ab293ac6373f415b92105182fa06da76e031fc23ac763730728f4d6c9`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-33:start -->
### Truth, covariance and common-reference geometry diagnosis

Updated 2026-09-29T04:42:14.236443+00:00.
#### Current conclusion and evidence levels

**Confirmed for all 491 prompt BDT-pass candidates:** the narrow fitted core is not an improvement of the full GEN mass-residual width. Q84−Q16 is 1.304509 MeV before and 1.449653 MeV after the D* refit. The earlier fitted DSCB FWHM change, 1.274656→0.610057 MeV, describes the core and is sensitive to the core/tail model. See the completed [matched bootstrap study](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/truth_geometry/../matched_shape_bootstrap/REPORT.md) for its existing statistical intervals; no new bootstrap or DSCB fit was run here.

**Confirmed intervention result for those same 491 prompt pass candidates:** replacing the MiniAOD track covariances with the official positive-definite prescription in the already-produced full D0/D* replay changes width68 only from 1.449653 to 1.450173 MeV and the fixed central-bin fraction from 19.9971% to 19.7911%. All 491 survive reconstruction. Original BDT membership and weights are fixed for this comparison. Thus positive-definite covariance replacement does not remove this observed core concentration. This does not establish full error coverage or equivalence to original RECO covariance.

**Interpretation:** vertex geometry and selection remain a plausible explanation. Matching the distributions of pre-refit D0/slow-pion pT and eta does not remove the prompt full-width broadening or central concentration in these descriptive comparisons. This is not proof that all kinematic confounding is removed, nor proof of a BDT error.

**Not yet established:** why the BDT-selected prompt population has the particular core response; whether the remaining angular changes improve track truth resolution; or the resulting fitted-yield bias. The common-reference diagnostic coverage is stated below. A decisive angular-truth comparison cannot currently use the stored GEN vertex as a detector-frame production point: the prompt preflight has both the selected GEN vertices and the separate GEN origin at (0,0,0), whereas its reconstructed primary vertex is displaced.


**Confirmed numerical path for 491 prompt BDT-pass candidates:** holding the fitted D* vertex fixed, the internally consistent Δm central-bin fraction changes **15.2590% → 18.7610% → 19.7911%** for input states → propagated input states → fitted states. Of the net 4.5321 percentage-point gain, 3.5021 points (77.3%) occur in the reference-point propagation step and 1.0300 points in the remaining fitted-state update. This fraction applies only to this fixed central-bin diagnostic. The endpoint was determined by the complete fit, so it is not an independent causal separation of vertex fitting from propagation. The nominal plotted observable uses a different stored D0-mass subtraction and has 19.9971% rather than 19.7911% after refit; both definitions are explicitly retained.

#### Coverage and frozen definitions

- Official prompt/nonprompt pThat2/pT4 only; pT7–10 GeV/c, |y|<0.3, centrality0–10%, tracker |cos(theta*)|0.6–0.8, DCA0–0.08 cm.
- Original manifest: 1,281 candidates, 1,258 events, 877 MiniAOD LFNs. All-source/private MC is outside this comparison.
- Saved successful baseline extraction: **1264/1,281** candidates; missing baseline snapshot jobs: NPR_26. All extracted candidates pass the same D*→D0+slow, D0→Kπ GEN ancestry and charge checks. This snapshot includes NPR_24 from its earlier successful baseline; a later retry encountered a file-open failure and must not erase the already-validated serialized snapshot. This is geometric/decay-chain association, not independent hit-level tracking truth.
- Common-reference geometry: **1264/1,281** candidates in **63/64** completed jobs. Cluster12764253, at most16 materialized jobs; source and worker revisions are documented below.
- Posdef reuse: 1247 baseline targets and 1238 matched corrected survivors. Missing corrected indices: [1386, 2477, 2876, 289, 806, 1027, 1217, 7829, 7830]. Pending intervention jobs: NPR_24, NPR_26. Do not merge missing corrected candidates into a successful paired comparison.
- Pass MVA≥0.95, fail 0<MVA<0.95. Membership, cosine bin and campaign weight are frozen from the original candidate. Corrected MVA is recorded separately: 9 cut crossings among paired survivors.
- The primary plotted observable is saved `mass - massDaugther1`; before is `deltaMOriginal`. Internal M(D0Refit+SlowRefit)−M(D0Refit) is a separate diagnostic. These definitions must not silently be interchanged.
- For comparison with the earlier 140–153 MeV study, both original stages must be in that window; excluded original index [1695]. Posdef intervention metrics use all common survivors with no new mass-window or corrected-BDT reselection.

#### GEN mass residuals

Residual = Δm_reco−[M(GEN D*)−M(GEN D0)], in MeV. width68 means Q84−Q16, not half of that interval. These are weighted descriptive values; bootstrap uncertainties remain in the earlier report.

|Family|Original BDT|N|Bias before→after|width68 before→after|Fraction with absolute residual >2 MeV before→after|
|---|---|---:|---:|---:|---:|
|PR|fail|224|0.2803→0.3147|1.2742→1.4452|8.48%→12.05%|
|PR|pass|491|0.2033→0.2324|1.3045→1.4497|8.25%→7.84%|
|NPR|fail|174|0.2642→0.3006|1.4606→1.5121|9.39%→10.57%|
|NPR|pass|374|0.1705→0.2513|1.3706→1.3191|9.00%→9.82%|

![GEN mass residuals](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/truth_geometry/gen_mass_residuals.png)

GEN decay four-vector closure: maximum component difference 1.8e-06 GeV. GEN Δm range 145.092964–146.282911 MeV; its event-specific value is subtracted. No invalid ancestry was silently replaced with another GEN candidate.

#### Kinematic common-support comparison

Separate within each family: build two or three weighted-quantile bins in each of original D0 pT, D0 eta, slow-pion pT and slow-pion eta. In cells occupied by both BDT groups, scale each group's weights to the smaller total weight. Empty-support cells have zero comparison weight and are explicitly recorded in `analysis_audit.json`. This is a coarse four-dimensional overlap check, not exact matching or a causal estimator. `kinematic_balance.csv` records residual standardized differences; `truth_metrics.csv` gives N, sumw, sumw2 and Neff for each comparison. Neff is not a candidate count.

![Kinematic overlap](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/truth_geometry/kinematic_overlap.png)

#### Covariance intervention and limitations

Of 1264 baseline candidates, 833 have a nonpositive minimum eigenvalue of the saved slow-track covariance; 833 have one in the fitted slow-pion state covariance. These are covariance-validity diagnostics, not a count of proven biased mass measurements. The paired difference covariance is unavailable for all 1264 candidates. We do not label `(fitted-input)/input_sigma` a calibrated pull.

The previously submitted cluster12755181 changes the covariance via `pseudoPosDefTrack()` and reruns D0 and D*. Its existing outputs are reused here. No second intervention campaign was generated.

|Family|Original BDT|Paired N|width68 baseline→posdef (MeV)|Central fraction baseline→posdef|Nonpositive input covariances baseline→posdef|
|---|---|---:|---:|---:|---:|
|PR|fail|218|1.4415→1.4479|16.055%→15.596%|141→0|
|PR|pass|491|1.4497→1.4502|19.997%→19.791%|326→0|
|NPR|fail|166|1.5606→1.5188|15.314%→15.314%|114→0|
|NPR|pass|363|1.3018→1.3109|16.287%→16.007%|232→0|

![Posdef control](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/truth_geometry/posdef_control.png)

The fixed central bin is 145.288135593–145.508474576 MeV. 5 paired candidates change membership under the intervention. Maximum individual Δm change is 1.470089 MeV; this maximum is not a typical shift or a failure criterion. Maximum BDT change is 0.297440. The 9 missing corrected candidates and cut crossings prevent using this fixed-survivor check as full production yield closure.

#### Common-reference geometry: method and current status

The independent diagnostic reconstructs the same D0+slow-pion fit from exact track keys, recorded mass hypotheses and the same magnetic field. It records input/fitted seven-dimensional state and covariance, D0/D* vertices, vertex chi2/ndf, and both input trajectories propagated to the transverse PCA relative to the final D* vertex. This does not force an unfit trajectory to pass through that vertex.

The angular decomposition is `total = propagation + constraint` (with wrapped phi). Mass changes are likewise evaluated from the input pair, propagated input pair and fitted pair; slow-only and D0-only substitutions and their non-additive interaction are saved in `geometry_candidates.csv`. The propagation endpoint is already determined by the complete vertex fit. Therefore, a dominant propagation component does NOT mean that the vertex fit or its covariance played no role in determining that endpoint. These substitutions diagnose a calculation, not alternative physical reconstructions.

The common point is the D* production-vertex fit. The D0 flight line is propagated from its decay vertex; the slow pion is not forced to originate at the D0 decay vertex.

Collected 1264 records have 0 invalid propagation/GEN-chain entries and 0 missing baseline joins. Independent state reproduction maximum is 9.63e-09 in the stored component units. Angular components satisfy the decomposition identity to <1e−12.

**GEN-position restriction:** 718/1264 collected slow-pion GEN vertices are exactly zero. Raw propagation to that stored point is preserved in JSON for auditing but excluded from the scientific angular-truth summaries. A generator/mixing vertex-frame mapping or retained simulation-vertex information must be established before calling that reference the actual production point. GEN mass residuals do not depend on this unverified vertex position.

The MiniAOD event-content audit found `genParticles:xyz0:SIM`, so it was checked directly with interpreted FWLite ROOT in run1/lumi1719/event111474631. That product and the selected prompt D*/slow-pion vertices are (0,0,0), while the reconstructed PV is (0.0380753, −0.0181233, 1.810502) cm. Of 50,239 pruned GEN particles, 50,207 have nonzero vertices, so this is NOT a statement that all GEN vertices were dropped. The embedding/coordinate mapping for the selected signal needs verification. `gen_origin_preflight.json` and `miniaod_event_content.txt` preserve the actual products.

That file also declares `packedPFCandidateToGenAssociation` and `lostTracksToGenAssociation`. Both associations have size0 in the inspected event. Official [trackPrunedMCMatchTask](https://github.com/cms-sw/cmssw/blob/CMSSW_13_2_11/SimTracker/TrackAssociation/python/trackPrunedMCMatchTask_cff.py) uses the hit associator, but a declared product is not evidence of a populated hit-based match. No independent tracking-truth claim is made for this sample, and the one-event emptiness check is not generalized to every CMS MiniAOD.

![Angular decomposition](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/truth_geometry/angular_decomposition.png)

#### Physical interpretation and official source

At fixed daughter masses,

`M(D0+pi)^2 = mD0^2 + mpi^2 + 2 (ED0 Epi - |pD0| |ppi| cos(alpha))`.

Changing the D0–slow-pion opening angle can therefore change Δm while leaving the D0 invariant mass nearly unchanged. A common-vertex constraint also connects the slow track to the selected D0 flight geometry. The BDT selects candidates; this identity and connection do not prove that the BDT training or the covariance is defective.

CMS [Vertex Fit Track Refit, Introduction](https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideVertexFitTrackRefit#Introduction) describes track parameters as “re-estimated at the fitted vertex”. Its distinction between re-estimating parameters and repeating a hit-level track fit applies here. The pinned [CMSSW13_2_11 stateAtPoint implementation](https://github.com/cms-sw/cmssw/blob/CMSSW_13_2_11/RecoVertex/KinematicFitPrimitives/src/TransientTrackKinematicParticle.cc) explicitly calls `propagateToTheTransversePCA`. Neither official statement is evidence for a sample-specific cause; that evidence must come from the recorded comparisons.

#### Execution, revisions and remaining work

All changes are inside this campaign study and its isolated CMSSW runtime. Hashes verify D0Fitter.cc, DStarFitter.cc and TrackAndVertexUnpacker.cc remain unchanged. No new MC, BDT training, bootstrap, ROOT macro compilation or production treatment was introduced.

1. Initial preflight config converted floating ROOT track keys to `cms.uint32` without an explicit integer cast; fixed before processing events.
2. Diagnostic v1 used daughter double masses rather than production's float mass hypotheses. The 1e−8 state-reproduction check correctly caught differences. V2 uses the recorded production hypotheses and reproduces the preflight states with difference exactly zero; the tolerance was not relaxed. `CommonVertexProbe_v1.cc`, `preparation*.json` and `revision_v2_*.json` preserve provenance. Validation-failed held jobs were released only after fixing the cause.
3. Worker revision3 evaluates the CMSSW runtime after `scram b ProjectRename` and asserts the extracted local CMSSW_BASE. This avoids dependence on mutable libraries in the original EOS runtime. An independent temporary-directory preflight passed with exact state equality. A temporary bigbird10 connection/security-negotiation outage initially blocked the queue edit, then recovered: `revision_v3_queue_verified.json` confirms deployment and the completed PR_05 runtime receipt is `/srv/bundle/CMSSW_13_2_11`. Already-running processes were not stopped. Earlier validated outputs retain their explicit state-reproduction checks; runtime generation should be checked per-job. Revision2 also preserves outputs when validation fails.
4. Posdef NPR_24 and NPR_26 initially had remote FNAL file-open timeouts. Their errors were preserved and the two jobs retried. NPR_24 baseline then succeeded but corrected exited139; NPR_20 corrected had exited134 during shutdown. NPR_20/24 were released with the local-runtime worker after preserving errors. NPR_26's repeated file-open failure remains explicit, with no further automatic retries. Geometry PR_16/17 were likewise explicitly released after process failures with the immutable local-runtime worker; a release is not a successful replay. These failure modes do not establish a physics defect.

Final execution status: 63 geometry jobs completed and one (`NPR_26`) is held after a terminal FileOpenError. All prompt716 candidates are included; 17 nonprompt targets from that job remain excluded. The failing LFN ends in `/90000/8fc38fd0-8a08-4a04-9613-d7295b2d17d9.root`; the remote server was `cmsdcadisk.fnal.gov:1095` and the error was Operation expired, with no additional data servers found. The separate posdef campaign has 62 completed jobs and two file-open failures (`NPR_24`, `NPR_26`); NPR_24's latest failure is in `/2520000/61b91382-fc84-4dd2-95b5-04d64a4e756d.root`. Earlier process failures and later input failures are distinguished in `final_job_status.json`; stale corrected logs are not evidence about a newer attempt that failed during baseline. No jobs were arbitrarily stopped and no further automatic retries are scheduled.

Remaining: restore access to the missing input for complete nonprompt coverage; resolve the signal GEN vertex coordinate mapping before angular-truth claims; and consider mass-template/yield closure only when a specific validated response model is available. The earlier paired bootstrap did not establish a significant BDT-specific interaction, so descriptive changes here do not override that statistical conclusion. No large new production or retraining is justified by the current results alone.


#### Collected geometry sample: core decomposition

Prompt coverage is complete; nonprompt excludes the 17 targets in the failed input job. These three stages use the internally consistent D0/slow states, so their central fractions need not equal the nominal stored-mass observable exactly. The component RMS values are correlated and must not be added in quadrature.

![Core decomposition](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/bdt_refit_causality_20260929/truth_geometry/core_decomposition.png)

|Family|Original BDT|N|Stage|Central fraction|GEN residual width68 (MeV)|
|---|---|---:|---|---:|---:|
|PR|fail|225|before|16.000%|1.2746|
|PR|fail|225|propagated|16.000%|1.4541|
|PR|fail|225|after|15.556%|1.4587|
|PR|pass|491|before|15.259%|1.3045|
|PR|pass|491|propagated|18.761%|1.4448|
|PR|pass|491|after|19.791%|1.4486|
|NPR|fail|174|before|11.222%|1.4606|
|NPR|fail|174|propagated|15.331%|1.5170|
|NPR|fail|174|after|15.331%|1.5193|
|NPR|pass|374|before|11.147%|1.3706|
|NPR|pass|374|propagated|15.496%|1.3247|
|NPR|pass|374|after|16.072%|1.3176|

|Family|Original BDT|N|Component|RMS|
|---|---|---:|---|---:|
|PR|fail|225|dm_propagation|0.376644|
|PR|fail|225|dm_constraint|0.171666|
|PR|fail|225|eta_total|0.00362319|
|PR|fail|225|eta_propagation|5.07455e-09|
|PR|fail|225|eta_constraint|0.00362319|
|PR|fail|225|phi_total|0.005474|
|PR|fail|225|phi_propagation|0.0047018|
|PR|fail|225|phi_constraint|0.00284162|
|PR|pass|491|dm_propagation|0.43064|
|PR|pass|491|dm_constraint|0.148488|
|PR|pass|491|eta_total|0.00288105|
|PR|pass|491|eta_propagation|4.43624e-09|
|PR|pass|491|eta_constraint|0.00288105|
|PR|pass|491|phi_total|0.00581671|
|PR|pass|491|phi_propagation|0.00520386|
|PR|pass|491|phi_constraint|0.00243625|
|NPR|fail|174|dm_propagation|0.434213|
|NPR|fail|174|dm_constraint|0.182143|
|NPR|fail|174|eta_total|0.00334821|
|NPR|fail|174|eta_propagation|4.93435e-09|
|NPR|fail|174|eta_constraint|0.00334821|
|NPR|fail|174|phi_total|0.00566075|
|NPR|fail|174|phi_propagation|0.00495551|
|NPR|fail|174|phi_constraint|0.00236017|
|NPR|pass|374|dm_propagation|0.526662|
|NPR|pass|374|dm_constraint|0.2109|
|NPR|pass|374|eta_total|0.00389633|
|NPR|pass|374|eta_propagation|4.88999e-09|
|NPR|pass|374|eta_constraint|0.00389633|
|NPR|pass|374|phi_total|0.00638099|
|NPR|pass|374|phi_propagation|0.00578815|
|NPR|pass|374|phi_constraint|0.00260523|
<!-- source:SRC-33:end -->

<a id="src-34"></a>

## SRC-34 — cascade_method_review_20260929/REPORT.md

**기록 구분:** 조사 보고서·해석 (표본·시점별 결과). **원문:** [cascade_method_review_20260929/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/cascade_method_review_20260929/REPORT.md). **SHA256:** `90993400871fd02e9db68629c2f3a829d1c8531ffe2a9ca38810510bdea0f5b2`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-34:start -->
### Composite + track cascade reconstruction review (2026-09-29)

Read-only review; no reconstruction settings or production changed.

#### Direct examples in the preserved production source

Source: ../baseline_source/VertexCompositeAnalysis/VertexCompositeProducer/src/V0Fitter.cc, V0Fitter::fitAll.
- Lambda -> p pi kinematic vertex fit: lines 1294–1305.
- Lambda mass constraint: 1316–1325.
- Xi- -> Lambda pi-: construct bachelor pion + constrained Lambda at 1343–1346, KinematicParticleVertexFitter::fit at 1349.
- Omega- -> Lambda K-: construct bachelor kaon + Lambda at 1489–1492, fit at 1495.
- Lambda_c+ -> Lambda pi+ shares the Xi fit path, charge selection at 1337–1340.
- Lambda_c+ -> Ks p: Ks two-track fit at 799–805, Ks mass constraint at 818–825, composite + bachelor fit at 850–855; bachelor mass hypothesis selected earlier in the loop.

This is a user/CMS analysis package, not a claim that V0Fitter.cc is shipped unmodified in cms-sw/cmssw.

Our DStarFitter.cc baseline: daughters -> D0 fit at 597–605; tree top at 613; composite D0 + slow pion at 615–617; parent vertex fit at 619–621. No D0/Dstar mass or PV constraint is applied in this block. It passes the fitted KinematicParticle, not merely a p4 plus a vertex 3x3 covariance.

#### Official CMSSW evidence

CMSSW_13_2_11 contains HeavyFlavorAnalysis/SpecificDecay/interface/BPHBuToJPsiKBuilder.h, a B+- -> J/psi K+- builder using BPHDecayToResTrkBuilder and a J/psi constraint. This demonstrates composite-plus-track candidate construction; its short-lived J/psi geometry should not be equated with flying D0 geometry.
https://github.com/cms-sw/cmssw/blob/CMSSW_13_2_11/HeavyFlavorAnalysis/SpecificDecay/interface/BPHBuToJPsiKBuilder.h

CMSSW_13_2_X HeavyFlavorAnalysis/RecoDecay/src/BPHKinematicFit.cc, kinematicTree(name,KinematicConstraint*), explicitly fits the component, optionally mass-constrains it, obtains compTree->currentParticle(), appends it to kTail, and calls the vertex fitter again. The library also has setIndependentFit for a decaying daughter represented as an independently fitted particle.
https://github.com/cms-sw/cmssw/blob/CMSSW_13_2_X/HeavyFlavorAnalysis/RecoDecay/src/BPHKinematicFit.cc

CMSSW_13_2_11 RecoVertex/KinematicFitPrimitives/src/VirtualKinematicParticle.cc, stateAtPoint, propagates the composite state to transverse PCA relative to the requested point. Thus a composite at its decay vertex is not held at that position during the parent fit.
https://github.com/cms-sw/cmssw/blob/CMSSW_13_2_11/RecoVertex/KinematicFitPrimitives/src/VirtualKinematicParticle.cc

CMS published Xi- -> Lambda0 pi- reconstruction with a Lambda mass constraint and vertex probability requirement: Performance of the CMS tracking detectors from the 2009 LHC run, arXiv:1006.1123.
https://arxiv.org/abs/1006.1123

Official sequential-fit guide:
https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideKinematicVertexFit

#### Physical interpretation

The physical constraint is that the D0 flight trajectory, propagated backward from its decay vertex, and the charged slow-pion trajectory share the Dstar decay point. It is NOT a requirement that K, pi and slow pi originate at the D0 decay vertex. A neutral D0 is represented by a composite trajectory with correlated position, momentum and mass uncertainties. This is a legitimate cascade model if the input states/covariances and propagation are correct.

Back-to-back refers to the daughter momenta in the Dstar rest frame. The vertex fit works in the laboratory frame, where the measured near-parallel trajectories provide weak localization along the common flight direction. Both parallel and antiparallel straight lines are geometrically degenerate in the exactly collinear limit; the issue is collinearity, not a special failure caused by rest-frame back-to-back kinematics.

Existing EOS evidence, not a new computation: ../bdt_refit_causality_20260929/geometry_selection/REPORT.md gives prompt BDT-pass median opening angle 0.05671 rad, major vertex covariance scale 1.853 mm, and major-axis alignment with D0 0.9999969. The report states these are fitted covariance diagnostics, not validated truth coverage. Its charged slow-pion phi transport reproduces the propagation contribution to Delta-m to 6.32e-6 MeV across 1264 candidates. The propagation endpoint itself comes from the vertex fit; this does not exonerate the fit or its covariance.

Therefore precedent validates the reconstruction topology, not this sample's numerical conditioning, uncertainty coverage, or mass response. A tiny-Q Dstar does not inherit the performance of Xi/Lambda_c decays. A mass constraint on D0 is not equivalent to a production-point constraint and would also redefine the observable; it should not be added merely because Lambda/Ks examples use one.

Next discriminating checks: reconstructed Dstar vertex residual along the weak direction versus mapped GEN truth; signed D0 flight length (upstream/downstream ordering); slow-pion transport length and associated Delta-m change; uncertainty coverage with valid covariance. Only after that consider separate diagnostic alternatives. A PV constraint can be appropriate for a prompt-origin hypothesis but not indiscriminately for nonprompt Dstar from a displaced B decay. No such alternative is applied by this review.
<!-- source:SRC-34:end -->

<a id="src-35"></a>

## SRC-35 — causal_followup_20260919/DIRECTION_ABLATION.md

**기록 구분:** 조사 보고서·해석 (표본·시점별 결과). **원문:** [causal_followup_20260919/DIRECTION_ABLATION.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/causal_followup_20260919/DIRECTION_ABLATION.md). **SHA256:** `8077aeae3376d1c0b0610b27641486b7fd00865f4b766f920f78d28a4d67e6c1`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-35:start -->
### Slow-pion direction ablation in frozen nominal |cos theta*EP| 0.6--0.8

The same 501 PR pT4 candidates and weights are used throughout. The nominal cos bin is frozen; no candidate is re-binned by a reconstructed prefit or postfit cos in this test.

| State | Central count | Weighted central fraction |
|---|---:|---:|
| D0Internal + SlowOriginal | 76 | 15.1522% |
| D0Internal + SlowRefit | 99 | 19.7966% |
| D0Internal + slow(fitted pT and mass, original eta/phi) | 78 | 15.5561% |
| D0Refit + SlowRefit | 100 | 19.9985% |

There are 56 final fitted-children entrants relative to the original state. Of these, 55 are already central with D0Internal fixed and SlowRefit used. After restoring the slow pion's original eta/phi while retaining its fitted pT and mass, only 1 of the 56 remains central.

Therefore the slow-pion direction change is the dominant component of the observed Delta-m central-bin migration in this frozen nominal-cos sample. This combined eta/phi restoration does not identify eta and phi separately. It also does not test migration between alternative reconstructed cos definitions; that requires re-computing and re-binning cos for each D-star boost construction.
<!-- source:SRC-35:end -->

<a id="src-36"></a>

## SRC-36 — causal_followup_20260919/FULL_RECO_AVAILABILITY.md

**기록 구분:** 조사 보고서·해석 (표본·시점별 결과). **원문:** [causal_followup_20260919/FULL_RECO_AVAILABILITY.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/causal_followup_20260919/FULL_RECO_AVAILABILITY.md). **SHA256:** `4a60d6c57d3ec6953f639177fcd264b577331f34090ddaa89f7ab0d25522cae3`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-36:start -->
### Packing 전 full-RECO covariance 가용성 확인

대상 official PR pT4 MiniAODSIM:

`/promptDStarToD0PiToKPiPi_pThat-2_pT-4_TuneCP5_5p36TeV_pythia8-evtgen/HINPbPbSpring23MiniAOD-132X_mcRun3_2023_realistic_HI_v9-v2/MINIAODSIM`

확인 결과:

- DAS/DBS에서 이 dataset 및 대표 MiniAOD file의 parent가 반환되지 않는다.
- 같은 primary dataset 이름으로 공개된 `AODSIM` 또는 `GEN-SIM-RECO` dataset은 없고, 조회되는 tier는 위 `MINIAODSIM`뿐이다.
- 원본 MiniAOD의 process history에는 RECO와 PAT 단계가 남아 있지만, event content에는 `generalTracks`가 없다. 새 probe에서도 검사한 모든 대상 event에서 `general_tracks_available=false`인지 검증한다.
- `packedPFCandidateToGenAssociation`과 `lostTracksToGenAssociation` branch는 존재하지만, 명시적으로 입력 `PAT` process를 읽어도 대표 event에서 association size가 0이다. 전체 대상에서 같은지 최종 validation에 기록한다.

따라서 현재 공개·보관된 입력만으로 동일 track의 packing 전 `generalTracks` covariance를 회수하여 reference covariance만 바꾸는 exact paired replay를 만들 수 없다. MiniAOD packed covariance를 임의 SPD 행렬로 바꾸는 검사는 full-RECO reference가 아니며 모델 의존적인 sensitivity test가 되므로 이번 검증에는 적용하지 않는다.

사용한 확인 명령의 의미:

- `dasgoclient --query='parent file=...'`
- `dasgoclient --query='parent dataset=...'`
- 같은 primary에 대한 `*/AODSIM`, `*/GEN-SIM-RECO`, `*/MINIAODSIM` dataset 조회
- `edmDumpEventContent`로 `generalTracks`, `TrackingParticle`, track-to-GEN association 확인
- `edmProvDump`로 `RECO`/`PAT` process history와 association producer 설정 확인

<!-- source:SRC-36:end -->

<a id="src-37"></a>

## SRC-37 — causal_followup_20260919/REPORT.md

**기록 구분:** 조사 보고서·해석 (표본·시점별 결과). **원문:** [causal_followup_20260919/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/causal_followup_20260919/REPORT.md). **SHA256:** `9078cff02541ca58b508949f7784838ed145aa2645c552c5742216b0a6b6eb7b`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-37:start -->
### PR pT4 원본 MiniAOD causal follow-up

#### 범위와 검증

문제 nominal cos bin 0.6–0.8에서 중앙 bin에 fit 전 또는 후 한 번이라도 들어간 132후보를 전부 검사했다: entered 56, left 32, stayed 44. 이들과 같은 GEN D*를 공유하는 선택 후보 9개를 추가했고, 중앙 밖에서 slow-pion 방향 변화가 가장 큰 악화 20개와 개선 20개를 대조군으로 추가했다. 총 181후보, 171 event, 155 원본 MiniAOD file이다.

원래 PR pT4 production PSet과 fitter를 유지하고 각 후보의 D0+D* fit을 독립 replay했다. 후보 선택, covariance, constraint, nominal cos, weight 및 production output은 변경하지 않았다. 모든 output은 diagnostic candidate index로 연결했다.

- 읽은 후보: 181 / 181
- 성공 batch: 16 / 16
- 모든 replay valid: True
- 저장된 slow fitted state와 replay 최대 차이: 1.46e-10
- 모든 후보에 D* inverse trace 존재: True
- 기록된 inverse 연산 전부 성공: True

#### 실제 inverse 경로

이 표본에서는 원본 slow covariance가 indefinite인 126후보 모두 D* replay에서 general inverse fallback을 사용했고, PSD 55후보는 한 번도 사용하지 않았다. 따라서 대표 8후보에서 본 경로가 이번 targeted 표본 전체에서 재현된다.

중앙 유입 56후보에서는 indefinite 39개 중 39개, PSD 17개 중 0개가 fallback을 사용했다. 중앙 유입 여부와 관계없이 covariance 그룹에 따라 같은 경로가 나타나므로, fallback이 중앙 유입의 충분조건은 아니다. PSD 중앙 유입도 그대로 존재한다.

| 표본 | N | D* fallback 후보 | 실제 slow 6×6 indefinite | nominal D* match 복수 | 대안 pion original / fit |
|---|---:|---:|---:|---:|---:|
| 전체 targeted | 181 | 126 (69.6%) | 126 | 0 | 22 / 25 |
| entered | 56 | 39 (69.6%) | 39 | 0 | 4 / 5 |
| left | 32 | 22 (68.8%) | 22 | 0 | 4 / 5 |
| stayed | 44 | 29 (65.9%) | 29 | 0 | 5 / 5 |
| outside tail controls | 49 | 36 (73.5%) | 36 | 0 | 9 / 10 |
| PSD | 55 | 0 (0.0%) | 0 | 0 | 9 / 10 |
| indefinite | 126 | 126 (100.0%) | 126 | 0 | 13 / 15 |

`실제 slow 6×6 indefinite`는 D* fit의 `predictedStateWeight`에 들어간 charged slow track perigee covariance를 대각 정규화한 최소 고유값 < −1e−10 기준이다. `D* fallback`은 같은 replay의 `KalmanVertexUpdator_S`가 Cholesky에 실패해 `general_Invert_fallback`을 실제 호출한 경우다.

#### GEN matching 대안과 중복

기존 analyzer가 선택한 pruned GEN D* chain 외에 같은 charge slow pion 및 완전한 D0+slow topology를 원본 MiniAOD의 전체 `prunedGenParticles`에서 다시 열거했다. Nominal fitted daughters로 동시에 통과하는 D* chain이 복수인 후보는 0 / 181개다. Accepted slow pion 외에 ΔR<0.03인 같은-charge pion이 있는 후보는 original 기준 22개, fit 기준 25개다. 후보별 index와 ΔR은 `candidate_details.csv/json`에 있다.

Target nonoutside 후보와 연결된 동일 GEN D* 중복 그룹은 10개이며, 10개 그룹 모두에서 선택 표본의 모든 partner를 검사했다. 모든 그룹의 reco slow track이 서로 달랐는가: True. 모든 후보가 유일한 nominal D* chain만 통과했는가: True. Accepted pion 외 대안 ΔR match가 있는 중복 그룹 수: 3. Tail control에서 우연히 잡힌 불완전 duplicate 그룹은 이 집계에서 제외하고 `duplicate_gen_groups.json`에 함께 기록했다.

입력 `PAT` process의 `packedPFCandidateToGenAssociation`과 `lostTracksToGenAssociation`을 명시적으로 읽었지만 association size는 각각 [0]와 [0]였다. 독립 track truth association을 얻은 후보는 0개다. 따라서 동일 GEN D* 중복은 서로 다른 reco slow track이 같은 geometric decay match를 통과했다는 뜻이며 hit-level truth로 어느 track이 맞는지 결정할 정보는 이 MiniAOD에 없다.

#### Packing 전 covariance

검사한 후보에서 `generalTracks`가 event content에 존재한 수는 0개다. DAS/DBS는 이 MiniAOD file/dataset의 parent를 반환하지 않고, 같은 primary 이름의 공개 AODSIM 또는 GEN-SIM-RECO dataset도 없다. 그러므로 동일 track의 packing 전 full-RECO covariance를 회수한 exact paired replay는 현재 입력으로 수행할 수 없다. 세부 확인은 `FULL_RECO_AVAILABILITY.md`에 기록했다.

#### 결론의 범위

이번 검사는 covariance pathology와 실제 fallback의 연결을 대표 8후보에서 181개의 targeted 후보로 확장한다. 동시에 entered, left, stayed에 fallback 후보가 모두 존재하고 PSD entered 후보도 존재하므로, indefinite covariance 또는 fallback 하나만으로 중앙 집중을 설명할 수 없다.

GEN 대안 chain 검사는 geometric matching의 모호성을 직접 센 결과다. 독립 track truth association이 비어 있고 full-RECO parent가 없으므로, 남은 인과 검증에는 원래 production 단계에서 AOD/RECO 및 TrackingParticle association을 별도로 보존한 표본이 필요하다. Reference 없이 covariance clipping/SPD replacement를 적용하는 것은 모델 의존 sensitivity test이므로 이 결과에는 포함하지 않았다.

#### 산출물

- `candidate_details.csv/json`: 후보별 source, GEN 대안, replay, inverse 경로
- `duplicate_gen_groups.json`: 동일 GEN D* 그룹별 partner 및 slow track 수
- `results.json`: validation과 그룹 요약
- `verification.json`: 독립 산출물·reference·경로 일관성 검사
- `target_manifest.csv/json`, `jobs.json`: 고정된 대상과 실행 단위
- `work/job_*.jsonl`, `work/job_*_inverse.jsonl`, `work/job_*_FJR.xml`, `work/job_*.log`: 원본 실행 기록
- `CausalFollowupProbe.cc`, `InverseTrace.cc`: 사용한 계측 소스
<!-- source:SRC-37:end -->

<a id="src-38"></a>

## SRC-38 — fit_stability_20260929/PLAN.md

**기록 구분:** 계획·제안 원문 (완료의 증거 아님). **원문:** [fit_stability_20260929/PLAN.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/fit_stability_20260929/PLAN.md). **SHA256:** `9938a0a864a2cd31841ba1332aa2efd64bb22a13a862cdc97f00e2cfc1702c2c`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-38:start -->
### D* numerical stability diagnostic, 2026-09-29

Authorized scope: same-input MC numerical validation of the second D* vertex fit. No data/MC production replacement or submission. Final adoption would require the same candidate reconstruction change on data and MC from available MiniAOD and reevaluation of downstream response/efficiency/BDT dependencies.

Inputs: all 1264 existing truth_geometry saved pre-D* input kinematic states and 7x7 covariances, 716 PR + 548 NPR, original tracker-axis |cos(theta*)| 0.6–0.8 subset. This is NOT the earlier five-bin EP study. Original identities, scores, weights and selected membership are frozen; 17 previously missing NPR candidates remain missing.

Each variant reconstructs a fresh VirtualKinematicParticle (D0) from its saved composite state; the slow pion is recreated through reco::Track -> TransientTrack -> KinematicParticleFactoryFromTransientTrack using the saved unpacked track reference position/momentum/charge and 5x5 covariance. This is not recovery of absent original full RECO tracks. The slow kinematic state and 7x7 covariance must match the saved input. Direct reconstruction of the slow 7x7 state was abandoned after a strict closure check found small differences from the trajectory representation; its artifacts are retained in state_reload_attempt/. No K/pi sub-tree is restored: the D0 composite current state is the input being held fixed. Numerical closure against previous full replay is mandatory for vertex, children, mass and covariance before use. Same CMSSW_13_2_11, magnetic field ESProducer and GT 132X_mcRun3_2023_realistic_HI_v9, run 1. No arbitrary covariance repair, constraints, GEN-informed seed or outcome-based selection.

Tests:
- official library default, and instrumented default to test zero-change closure;
- XY convergence thresholds 100/10/1 micrometres, iteration limits 100/300 separately;
- XYZ thresholds 100/10/1 micrometres, 300 iterations, plus 0.1 micrometre plateau check;
- default linearization seed shifted along prefit reconstructed D0 direction by +/-1 and +/-5 mm, XYZ 1 micrometre threshold, 300 iterations.

Code clones only KinematicParticleVertexFitter and SequentialVertexFitter under unique class names. Diff retains the original finite loose seed covariance (10000 cm² diagonal) and no physical prior. Instrumentation records per-iteration step,x,y,z,delta_xy,delta_z,delta_3d,chi2,valid, reset count and stop reason. State/vertex errors stay those produced by the same smoother. Limits/invalid results remain explicit. Stable results need not be physically accurate; indefinite covariance remains a separate defect.

Artifacts are stored on EOS. No full-chain detector simulation/reconstruction or new CRAB/Condor submission is part of this task.
<!-- source:SRC-38:end -->

<a id="src-39"></a>

## SRC-39 — fit_stability_20260929/REPORT.md

**기록 구분:** 조사 보고서·해석 (표본·시점별 결과). **원문:** [fit_stability_20260929/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/fit_stability_20260929/REPORT.md). **SHA256:** `c86ce0a8f2c808c0b5bb36a9af405762ff82d606550c40e76638dca0ebb457aa`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-39:start -->
### D* second-vertex-fit numerical stability — 2026-09-29

#### 결론

**수렴 정밀도를 높이는 것만으로 문제 중앙 집중이나 큰 vertex truth 잔차는 해결되지 않았다. 대부분의 후보는 수치적으로 안정적이지만, 소수에서 조기 종료·시작점 의존성·비물리적 valid 결과가 확인됐다. 따라서 fitter가 전부 정상이라고도, 단순 수렴 설정 변경이 해결책이라고도 결론낼 수 없다.**

원본 production은 변경하지 않았다. Data/MC 재생산도 제출하지 않았다. Fit 변경을 채택하면 data와 MC 모두 같은 MiniAOD→후보 reconstruction을 다시 실행하고, 효율·질량/각도 응답·영향받는 BDT 입력을 재평가해야 한다. 이 변경만을 위해 detector RECO나 MC GEN/SIM부터 재생산하는 것은 아니다.

#### 범위와 재현 검증

기존 geometry 표본 1,264개(PR 716, NPR 548), 15설정 = 18,960 fit 호출. Reco pT 7–10 GeV, |y|<0.3, centrality 0–10%, **tracker-axis** |cosθ*| 0.6–0.8, 원래 MVA>0 후보. PR pass/fail=491/225, NPR=374/174. Pass는 MVA>=0.95, fail은 0<MVA<0.95. 기존 누락 NPR 17개는 이번에도 없는 상태이며 다섯 EP cos 구간 전체 결과가 아니다. 기존 후보·weight·score·cos 구간을 고정했다.

CMSSW_13_2_11, 원래 DD4hep magnetic-field ESProducer와 132X_mcRun3_2023_realistic_HI_v9 GT를 사용했다. D0의 fit 직전 7상태/7×7 covariance를 복원하고, slow pion은 저장된 unpacked track 기준점·momentum·charge·5×5 covariance로 reco::Track→TransientTrack→kinematic particle 경로를 재현했다. 없는 full RECO track을 복구했다는 뜻은 아니다. K/pi subtree는 복원하지 않고 D0 composite 입력을 고정했다. 매 fit은 새 객체로 시작한다.

전체 1,264개에서 원래 입력 상태/covariance, fitted daughter 상태/covariance, vertex 위치/covariance의 최대 차이는 **0**. Parent covariance 차이 최대 5.07e-16, Δm 차이는 3.55e-12 MeV로 부동소수점 합산 수준. 진단용 default와 공식 default의 daughter/vertex 차이도 0. `validation.json`과 `closure.csv` 참조.

처음 slow 7×7 상태만 직접 복원한 시도에서는 trajectory 표현 변환에 따른 작은 차이가 남아 사전 기준을 통과하지 못했다. 그 결과는 채택하지 않았으며 `state_reload_attempt/`에 별도 보관했다. 아래 모든 결과는 track 경로로 정확히 재현한 최종 실행이다.

#### A/B: 반복 한도·종료 기준

기본 설정은 transverse 이동 100 μm, 최대 100회. 공식 코드를 별도 이름으로 복제해 종료 기준·시작점·기록만 바꾸었으며 원본 소스와 diff를 보존했다. 원래 loose seed covariance(대각 10000 cm²)는 그대로이며 새 mass/PV constraint나 covariance 보정은 없다.

|설정|Valid / 1264|최대 반복|Δm 변화 절댓값 중앙값 / 95% / 최대 (MeV)|0.01 MeV 초과 후보|
|---|---:|---:|---:|---:|
|XY 100 μm, 300회|1264|18|0 / 0 / 0|0|
|XY 10 μm, 300회|1264|21|4.22167e-05 / 0.00146289 / 0.219967|7|
|XY 1 μm, 300회|1264|24|5.94899e-05 / 0.00148069 / 0.219953|7|
|XYZ 100 μm, 300회|1264|18|0 / 0 / 0.00284684|0|
|XYZ 1 μm, 300회|1264|24|5.94899e-05 / 0.00148069 / 0.219953|7|
|XYZ 0.1 μm, 300회|1261|300|5.80554e-05 / 0.001471 / 0.219946|7|

최대 반복만 100→300으로 늘리면 모든 결과가 동일하다. 원래 default도 최대 18회에서 끝났으므로 반복 한도 부족이 주된 원인이 아니다. XYZ 1 μm에서 7개가 0.01 MeV 이상 바뀌며 2개는 0.1 MeV 초과. 더 엄격한 0.1 μm 기준에서는 3개가 300회 한도에 도달했다. 기준을 무조건 엄격하게 하는 것이 개선은 아니다. 실패 후보는 제거/대체하지 않고 기록했다.

#### 중앙 bin과 폭

동일 fitted-children 정의 Δm=M(D0+slow)−M(D0), 중앙 bin [145.28813559322035,145.5084745762712) MeV. Nominal 혼합 정의로 바꾸지 않았다. 아래는 baseline→XYZ 1 μm 비교이며 모든 해당 후보가 valid이므로 분모도 같다. 가중 Q84−Q16은 기존 분석과 같은 empirical CDF 보간법.

|그룹|N|중앙 비중 baseline→tight|중앙 유입/유출|중앙 68% 폭 baseline→tight (MeV)|
|---|---:|---:|---:|---:|
|PR_pass|491|19.791067% → 19.791067%|0 / 0|1.448572 → 1.448741|
|PR_fail|225|15.555556% → 16.000000%|1 / 0|1.458693 → 1.458683|
|NPR_pass|374|16.072272% → 16.072272%|0 / 0|1.360782 → 1.360476|
|NPR_fail|174|15.330794% → 15.330794%|0 / 0|1.566590 → 1.566709|
|PSD|431|16.673687% → 17.064584%|1 / 0|1.416362 → 1.416394|
|indefinite|833|19.124655% → 19.124655%|0 / 0|1.483379 → 1.481874|

**PR pass의 중앙 집중은 이 변경으로 사라지지 않는다.** 여기의 491개는 이전 EP 기준 501개와 다른 tracker-axis 표본이다. 표본을 혼동해 이전 20.00%와 직접 차이를 해석하지 않는다. 모든 variant의 paired-valid 분모/실패 수는 mass_response.csv와 summary.csv에 있다. 이번에는 FWHM/DSCB를 재적합하지 않았다.

#### C: 시작점 의존성과 예외

시작점을 reco D0 방향 ±1,±5 mm로 이동하고 XYZ 1 μm 조건을 동일하게 적용했다. GEN 위치를 시작점에 쓰지 않았다. Default seed의 tight 결과까지 포함한 다섯 시작점 사이의 범위를 `seed_spread.csv`에 기록했다.

|후보|분류|관측|
|---|---|---|
|2016|PR fail, slow PSD|Default는 첫 반복의 이동이 30.52 μm여서 종료. Tight는 8회 반복 후 vertex가 8.457 mm 이동하고 Δm 145.180997→145.400950 MeV. Tight의 서로 다른 시작점 해도 최대 16.535 mm 떨어짐. PSD만으로 유일한 vertex 해를 보장하지 않음.|
|1178|PR pass, slow indefinite|Default 1회→tight 18회, Δm 변화 +0.110173 MeV. 시작점에 따라서도 서로 다른 vertex로 수렴.|
|9209|NPR pass, slow indefinite|+5 mm 시작점에서 default 대비 Δm +3.027527 MeV, vertex 약 29.22 mm 이동. **valid이지만 χ²=−12.734**. 물리적으로 신뢰 가능한 대안 해/개선으로 취급할 수 없음.|
|10257|NPR pass, slow indefinite|일부 시작점은 300회 한도로 실패. 기존 default vertex covariance도 positive definite가 아니며 이 flag를 보존.|

이들은 진단용 입력 변화에 대한 반응이다. 크게 바뀐 해가 더 정확하다는 뜻도, 큰 Δm 변화가 원래 중앙 peak의 주원인이라는 뜻도 아니다. 후보의 run/lumi/event/LFN/track key는 manifest, 모든 부정 χ²·비양정 vertex covariance·실패와 큰 질량 변화는 flagged_candidates.csv에 있다. 최저 χ²나 중앙 mass에 가까운 결과를 골라 채택하지 않았다.

#### 큰 truth 잔차가 개선되는가

기존 64후보 pilot의 **background-reference 전이 가정에 조건부인** truth 추정치를 재사용했다. 원본 event의 smeared HepMC/hit-level matching을 새로 확인한 결과는 아니다.

|후보|잔차 baseline→XYZ 1 μm (mm)|pull baseline→XYZ 1 μm|
|---|---:|---:|
|3148|-10.813297 → -10.813578|-6.588232 → -6.588492|
|10270|4.457386 → 4.457545|5.184165 → 5.176590|

두 대표 큰 잔차는 거의 그대로다. 이 후보들은 이번 수렴 설정/시작점 검사 범위에서는 **안정적으로 큰 잔차를 가진 해**이며, 정밀도 설정만으로 해결되지 않는다. 전체 오차 calibration 완료를 의미하지 않는다.

#### 의사결정

이번 결과로 전체 data/MC를 tighter fit으로 재생산할 근거는 없다. 수치 민감 후보를 별도로 식별할 근거는 생겼지만, 후보 제거·오차 clipping·PV 강제는 적용하지 않았다. 두 번째 D* fit을 생략하는 4번 대안은 여전히 비교할 가치가 있으나, 채택 전 제한된 data/MC 재처리에서 효율·배경·질량/각도 응답을 함께 검증해야 한다. 현 진단은 fit 전부터 고정한 후보에 대한 비교라 새 reconstruction의 전체 효율을 측정하지 못한다.

#### 파일과 재현

`run_cfg.py`, `env.sh`, 독립 `CMSSW_13_2_11/src/Diagnostics/FitStability/plugins/`가 실행 코드다. `states.json`/`candidate_manifest.json`/`candidate_metrics.csv`가 frozen 입력. `results.jsonl`은 전체 상태·covariance·iteration 로그, `candidate_comparison.csv`/`summary.csv`/`mass_response.csv`/`pilot_truth_comparison.csv`가 수치 결과. `fit_stability.pdf`는 분포·민감 후보 반복 경로다. `source_provenance.json`, `artifact_checksums.json`과 빌드·cmsRun 로그를 보존했다. 초기 개발 과정의 실패/수정 로그도 현 디렉터리에 남아 있다.
<!-- source:SRC-39:end -->

<a id="src-40"></a>

## SRC-40 — handoff_replay_20260919/REPORT.md

**기록 구분:** 조사 보고서·해석 (표본·시점별 결과). **원문:** [handoff_replay_20260919/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/handoff_replay_20260919/REPORT.md). **SHA256:** `cd2edd84d334d3751a3725412d606fde79154d9efb677ac2a1d1ee46304ea22f`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-40:start -->
### 대표 8후보 production 재현 결과

작성: 2026-09-18 UTC (전달 패키지 이름은 20260919). 입력 패키지의 11개 checksum 모두 일치.

#### 결론

요청한 8개 원본 MiniAOD event를 각각 실제 production PR4/NPR4 PSet으로 재실행했다. 전달된 6 state × 8 fields × 8 candidates = 384개 값이 모두 정확히 일치했다. 원본 slow covariance 25개 원소, run/lumi/event, diagnostic candidate index 및 세 track key도 일치한다. 별도 probe에서 factory/fit 호출을 독립 반복한 state 최대 차이는 6.75016e-13, vertex 최대 차이는 1.37564e-15였다.

문제 cos 구간의 PR/NPR 중앙-bin 유입 후보에서도 **indefinite covariance → fit 직전까지 유지 → 실제 일반 역행렬/fallback 성공 → valid fit → slow 방향 변화 및 중앙 유입**이 재현됐다. 이전의 '문제 cos 구간 밖 후보만 검증했다'는 제한은 이 8후보에 대해 해소됐다.

그러나 covariance pathology가 중앙 돌출을 일으킨다는 인과 결론은 아직 아니다. PSD 대조 2개도 중앙으로 유입되고, indefinite 후보에는 중앙 잔류·유출이 있으며 다른 cos 구간에서도 같은 메커니즘의 이동이 있다. 따라서 음의 covariance는 개별 중앙 유입의 필요조건도 충분조건도 아니다. 대표 8개는 의도적 표본이므로 여기서 빈도/유의도/전체 돌출 크기를 추정하지 않는다.

기존 producer/fitter 소스, library, production PSet, constraint, covariance, CRAB production은 변경하지 않았다. 별도 진단 라이브러리는 기존 CMSSW 역행렬 연산을 그대로 수행하면서 경로를 기록한다. clipping, SPD 대체, 후보 제거, mass/PV constraint 추가는 하지 않았다.

#### 고정된 분석 정의

전달된 nominal 후보 집합, nominal cos bin, Psi2Flat_Trk 및 weight_pb를 그대로 유지했다. 후보 재선택/재binning/재weight를 하지 않았다. reconstructed nominal cos와 전달된 값의 최대 차이는 2.61817e-6으로 전달 패키지 검증 수준 내다. cos 차이를 이용해 bin을 바꾸지 않았다.

중앙 Δm bin: [0.14528813559322035, 0.1455084745762712) GeV. 다음 표의 same-D0 값은 diagD0Internal p4와 mass를 고정한다. '방향 복원'은 fitted slow pT + original eta/phi + fitted slow mass이며 새 vertex fit이 아니다. 모든 판정은 반올림 전 double로 했다.

#### 후보별 Δm와 방향 변화

단위: Δm MeV; eta 무차원, phi rad.

|label|event / diag index|before: original slow|fitted slow, same D0|eta/phi 복원, same D0|nominal legacy|delta eta|delta phi|
|---|---|---:|---:|---:|---:|---:|---:|
|PR_target_entered_indefinite|459490357 / 10|145.066504|145.409036|145.083641|145.408977|+0.004336791|-0.002190027|
|PR_target_entered_PSD_control|400061033 / 12|144.637196|145.508471|144.616143|145.505960|-0.007594097|+0.008549852|
|PR_target_stayed_peak_indefinite|618328793 / 12|145.356544|145.345396|145.416743|145.346677|-0.010571786|+0.000916142|
|PR_target_left_peak_indefinite|410895098 / 12|145.298416|145.741628|145.286278|145.744311|-0.016364506|+0.000658950|
|PR_cos04_06_entered_indefinite|425542171 / 17|145.025212|145.316358|145.022107|145.318047|-0.002163119|+0.001984097|
|PR_cos08_10_entered_indefinite|206158633 / 0|145.040034|145.422113|145.038347|145.414322|+0.000235820|+0.002361228|
|NPR_target_entered_indefinite|141891265 / 26|143.589745|145.352202|143.385819|145.390389|+0.054150315|+0.000251515|
|NPR_target_entered_PSD_control|60270446 / 45|146.387305|145.313348|146.388622|145.307713|-0.000856727|-0.007344209|

PR PSD control의 same-D0 fitted 값 145.508470817658 MeV는 상한 145.508474576271 MeV 바로 아래다. 반올림한 표로 bin 판정을 다시 하면 안 된다.

전달된 before 값과 이 표의 직접 same-D0 계산 차이는 최대 6.17e-11 GeV이며 D0 상태 표현/질량 연산 수준의 차이다. 전달 nominal 및 direction-restored Δm는 8개 모두 차이 0이다.

#### fit 직전 covariance와 실제 inverse 경로

표의 고유값은 원본 및 7x7 covariance를 각각 대각 정규화한 뒤 계산한다. 7x7 PSD control의 약 ±1e-16 고유값은 track 5D→Cartesian 6D 표현의 rank와 수치 오차 수준으로, 유의한 음수와 구분했다.

|label|min eig original 5x5|min eig prefit 7x7|D* S inversion Cholesky / general fallback 호출 수|D* chi2 / ndf|valid|
|---|---:|---:|---|---|---|
|PR_target_entered_indefinite|-0.646134435|-0.756947892|5 / 5|0.111752108 / 1|true|
|PR_target_entered_PSD_control|0.331864015|-2.69463971e-16|10 / 0|24.3784657 / 1|true|
|PR_target_stayed_peak_indefinite|-0.646143769|-0.704562227|3 / 3|0.321638912 / 1|true|
|PR_target_left_peak_indefinite|-0.646140705|-0.65855952|3 / 3|0.716203988 / 1|true|
|PR_cos04_06_entered_indefinite|-0.819484224|-0.0161939963|3 / 3|0.588033915 / 1|true|
|PR_cos08_10_entered_indefinite|-0.712951824|-0.0993967894|3 / 3|0.00903280731 / 1|true|
|NPR_target_entered_indefinite|-0.646119175|-0.677445736|7 / 7|9.16404438 / 1|true|
|NPR_target_entered_PSD_control|0.318743075|2.42513866e-16|6 / 0|0.502345085 / 1|true|

8개 모두 `diagSlowOriginalCov = pseudoTrack covariance = unpacked TrackRef covariance = TransientTrack::track covariance = initial curvilinear covariance`, 원소별 최대 차이 0. 이들 fit 입력에서 covariance 저장 오류는 배제된다.

factory 직전 7x7를 그대로 역행렬화하는 것이 아니다. fit의 linearized track은 mass를 포함하는 perigee 6x6 covariance로 바뀌며 `ExtendedPerigeeTrajectoryError::weightMatrix()`에서 일반 `cov.Inverse(error)`를 사용한다. slow pion의 이 **실제 inverse 입력 6x6**에도 indefinite 6후보에서는 유의한 음의 고유값이 남고 일반 inverse가 성공했다. PSD controls의 해당 6x6은 양의 고유값이다. 반복/linearization별 행렬과 eigenvalues는 comparison.json의 actual_slow_weight_inputs에 있다.

`KalmanVertexUpdator::positionUpdate` 및 `chi2Increment`의 S 행렬에서는 Cholesky를 먼저 시도하고 실패 시 일반 Invert를 사용한다. 위 호출 수는 이 두 지점의 실제 실행 로그다. 모든 기록된 inverse call이 성공했다. PSD 2후보의 D* S 계산은 모두 Cholesky; indefinite 6후보의 D* S 계산에는 실제 general fallback이 있다. 입력 covariance inverse와 S inverse를 혼동하지 않았다. 이 계측은 핵심 두 경로를 기록하며 CMSSW의 모든 행렬 연산을 전수 계측했다는 뜻은 아니다.

소스: CMSSW_13_2_11 `RecoVertex/KinematicFitPrimitives/interface/ExtendedPerigeeTrajectoryError.h:28`, `RecoVertex/KinematicFitPrimitives/src/ParticleKinematicLinearizedTrackState.cc:35`, `RecoVertex/KalmanVertexFit/src/KalmanVertexUpdator.cc:85,98,149,156`, `DataFormats/Math/interface/invertPosDefMatrix.h:9`. `InverseTrace.cc`에는 사용한 KalmanVertexUpdator 코드와 경로 logger가 함께 보관돼 있다. 역행렬 결과를 다른 값으로 대체하지 않는다.

valid는 fit goodness나 SPD의 보증이 아니다. 예를 들어 PR PSD control의 chi2/ndf는 24.3785/1이지만 기존 설정 VtxChiProbCut=0에서 선택된 후보이며, 이 검사에서 추가 cut을 적용하지 않았다.

#### Δm 정의를 분리한 대조

O = 최초 D0 fit 후 저장된 original D0 p4, I = diagD0Internal, F = diagD0Refit, S = diagSlowRefit, R = original slow-pion p4, P = diagDStarFit.

1. nominal legacy: `M(Plegacy) - M(O)`. Plegacy momentum은 fitted parent momentum이며 energy는 fitted D0 momentum+M(O), fitted slow momentum+고정 pion mass를 사용한 float 연산의 합.
2. final parent state: `P.mass - F.mass`. 최종 parent와 D0 child state의 invariant mass를 사용.
3. final children sum: `M(F.p4+S.p4) - F.mass`. `deltaMRefit`은 p4에서 mass를 계산하는 정의이고 직접 scalar state.mass를 사용할 때 마지막 자리 차이가 가능. 둘 다 comparison.json에 보존.
4. same-D0 original/fitted slow: `M(I.p4+R.p4)-I.mass` / `M(I.p4+S.p4)-I.mass`.
5. same-D0 direction restored: `M(I.p4+slow(fitted pT, original eta, original phi, fitted mass))-I.mass`.

코드 위치: [DStarFitter.cc:701](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:701) nominal p4, [DStarFitter.cc:805](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:805) slow daughter, [DStarFitter.cc:830](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:830) final state 저장, [DStarFitter.cc:842](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:842) original D0 daughter 유지, [PAT6RefitDiagnostics.icc:82](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PAT6RefitDiagnostics.icc:82) Δm 계산.

|label|nominal legacy MeV|final parent state MeV|final children sum MeV|deltaMRefit branch MeV|
|---|---:|---:|---:|---:|
|PR_target_entered_indefinite|145.408977101|145.409869981|145.409869458|145.409869458|
|PR_target_entered_PSD_control|145.505960135|145.491193038|145.491191947|145.491191947|
|PR_target_stayed_peak_indefinite|145.346676733|145.345768706|145.345769079|145.345769079|
|PR_target_left_peak_indefinite|145.744310599|145.744103220|145.744104139|145.744104139|
|PR_cos04_06_entered_indefinite|145.318047298|145.314653899|145.314653083|145.314653083|
|PR_cos08_10_entered_indefinite|145.414322059|145.421644236|145.421650970|145.421650970|
|NPR_target_entered_indefinite|145.390388683|145.396655233|145.396655339|145.396655339|
|NPR_target_entered_PSD_control|145.307713468|145.308499558|145.308500189|145.308500189|

Nominal/fit-state 차이를 오류라고 가정하지 않았고, nominal cos 재현에는 전달된 nominal mass를 사용했다. 방향 복원 결과의 중앙 유입/유출은 frozen cos 후보 집합 내부의 Δm 이동이다.

#### 동일성 검증과 ProductID 주의

production revision `1e271d7ad110b552b0b5cc70204c53a6fd5e7c66a5c32187374fdebf7a2a0227`. 159 source, 3 production library, 3 PSet checksum 모두 원래 manifest와 일치 (`provenance_check.json`). 8개 cmsRun 모두 exit 0.

진단 모듈/LD_PRELOAD 없이 첫 PR 문제 이벤트를 원래 PSet으로 한 번 더 실행했다. 5개 기존 tree의 825개 branch를 instrumented 실행과 비교했다. physics 및 fit/GEN/EP 값은 모두 정확히 같다. **차이는 diagKProductID/diagPiProductID/diagSlowProductID 세 가지뿐**으로, 원래 실행은 17, 검사 실행은 18이다. 추가 probe/input-copy를 가진 실행의 EDM product registry 번호가 달라진 것이다. diagnostic candidate index와 세 track key는 모두 동일하고 track의 covariance/kinematic 값도 동일하다. ProductID 정수를 독립 실행 간 전역 식별자로 비교하지 않았다. `uninstrumented_control_comparison.json`과 ROOT에 증거가 있다.

계측은 original source/library를 덮어쓰지 않는 별도 `RefitAudit/InverseTrace` library의 LD_PRELOAD 방식이다. `HandoffInputProbe`가 독립 재fit 직전에만 stage logging을 켠다. 원래 후보 생성 중에도 교체된 함수는 동일 연산을 수행하며, 그 영향은 전달된 384개 state 정확일치와 별도 uninstrumented control 비교로 확인했다.

#### 903후보 목록의 보조 확인 — 새 cmsRun 재현 아님

전달 CSV만 집계한 descriptive 결과를 `packet_903_summary.json`에 남겼다. 후보 수는 PR indefinite330 / PSD171, NPR indefinite253 / PSD149. PR에서는 indefinite 중앙 유입40/유출22, PSD 유입17/유출10; NPR에서는 indefinite 유입28/유출15, PSD 유입17/유출13이다. 이 분류는 원본 5x5 고유값과 CSV의 before/nominal Δm에 따른 것이며, 903개 fit 직전 7x7 검사를 수행한 것은 아니다. 이 숫자로 인과관계나 유의도를 주장하지 않는다.

#### 원인 구분과 다음 검사 제안 — 아직 실행하지 않음

확인된 사실: (a) branch 저장 단계에서 생긴 covariance 문제 아님, (b) 실제 target 후보에서도 indefinite 입력이 inverse 및 valid fit까지 통과, (c) fixed-D0에서 slow 방향 복원만으로 중앙 유입이 사라지는 사례 재현, (d) PSD 및 다른 cos control에도 방향 변화/유입 존재.

남은 가설: 잘못된 covariance가 angular response를 왜곡하여 중앙 집중을 증폭하는지, 또는 정상 vertex fitting/선택/kinematics와 함께 나타나는지. 입력이 이미 indefinite인 사실과 중앙 집중의 인과 원인은 별개다. 단순히 indefinite 여부만으로 fitted 후보를 제거하면 후보 집합이 바뀌어 비교를 흐리므로 여기서는 하지 않았다.

다음 단계로는 같은 event/track의 **packing 전 full-RECO covariance**를 확보할 수 있는지 먼저 확인하는 것이 좋다. 가능하면 같은 reference point/basis로 전파한 reference covariance만 바꾸는 작은 paired replay로, 원래 momentum·candidate set·cos bin·constraint를 고정하고 Δeta/Δphi/Δm와 fit failure를 모두 비교한다. 이것이 불가능하면 covariance 대체를 이용한 검사는 모델 의존적인 sensitivity test임을 명시하고 별도 설계해야 한다. 현재 어느 대체/clipping/재생산도 적용하지 않았다.

#### 산출물 및 재실행

- `comparison.json`, `comparison.csv`: 기대값 대조, Δm 각 정의, state/vertex, cov eigenvalues, inverse summary.
- `*_inverse.jsonl`: 단계별 실제 inverse 입력 행렬·경로·성공 여부.
- `<label>.jsonl`: unpacked/track/transient/7x7 covariance와 prefit/replayed/output state.
- `<label>.root`: 해당 이벤트의 기존 production 분석 ROOT (모든 후보 유지).
- `<label>_input.root`: 원본 MiniAOD 해당 이벤트만 복사한 EDM. 원래 LFN은 packet과 cfg에 기록. 이 복사본은 보관용이며 이번 검사 입력은 원본 remote LFN이다.
- `<label>_cfg.py`, `<label>_FJR.xml`, `<label>.log`: 정확한 설정과 cmsRun 기록.
- `source_snapshot/`, `configs_snapshot/`: 제출 source/설정과 별도 probe/inverse trace 소스 사본.
- `run_replays.py`: 동시에 2개 이하의 개별 이벤트 작업. 기존 환경 `scripts/env.sh`, CMSSW EL8 사용. 출력/작업 파일은 EOS 아래.
- `production_handoff/`: 원래 전달 패키지의 검증된 내용.

archive는 checksum manifest와 함께 제공한다. 재실행 시 env.sh의 workspace 절대 경로와 proxy 위치는 실행 환경에 맞춰 지정해야 한다. 대표 후보는 반드시 diagnostic_candidate_index로 선택한다.
<!-- source:SRC-40:end -->

<a id="src-41"></a>

## SRC-41 — handoff_replay_20260919/production_handoff/README.md

**기록 구분:** 정의·설계·재현 자료 (각 본문의 구현 상태 참조). **원문:** [handoff_replay_20260919/production_handoff/README.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/handoff_replay_20260919/production_handoff/README.md). **SHA256:** `76441acdbdb9dbb77e8c0f1169172d9cc8236371d92dc377fa91d2fa282859e4`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-41:start -->
### Production agent follow-up: actual target candidates

#### 요청

앞서 확인한 입력 covariance 문제를 실제 돌출 구간에 연결해 주세요. 우선 대표8개를 원본 MiniAOD에서 기존 설정 그대로 재현하고, nominal production 변경 없이 단계별 상태를 저장해 주세요. 대표 후보들은 진단 목적의 극단값/대조 선택이며 빈도 추정용 무작위 표본이 아닙니다.

1. 각 후보에서 pseudoTrack5x5 → transient state → mass를 포함한 fit 직전7x7의 covariance, 정규화 고유값, 실제 사용한 inverse 경로/성공 여부를 기록해 주세요.
2. Slow pion의 fit 직전/최종 px,py,pz,E와 eta/phi 변화, D0/parent state, fit chi2/ndf를 재현하고 JSON expected_states와 대조해 주세요. Covariance의 음의 고유값이 남은 상태로 valid 결과를 만드는지 후보별로 확인해 주세요.
3. Delta-m를 (a) 기존 nominal 저장 정의, (b) 최종 parent/child 일관된 정의, (c) 같은 D0에 original/fitted slow pion을 각각 붙인 정의로 분리해 주세요. 상태 혼합을 오류라고 미리 가정하지 말고 각 정의의 수식을 코드 줄과 함께 명시해 주세요.
4. 아래8개만으로 빈도나 원인을 확정하지 마세요. Indefinite/PSD, 중앙 유입/유출/잔류, 다른 cos controls에서 반복되는 현상인지 비교한 뒤 다음 개입 검사를 제안해 주세요. Covariance clipping·후보 제거·mass constraint 추가·전체 재생산은 아직 하지 마세요.

#### 고정한 분석 정의

- PR/NPR pT4, reco7–10 GeV, abs(y)<0.3, centrality0–10%; target abs(cos(theta*_EP))0.6–0.8. Target 후보 PR501+NPR402=903개.
- 중앙 Delta-m: [0.14528813559322035, 0.1455084745762712) GeV. 경계는 np.linspace(0.140,0.153,60)[24:26]이다.
- 진단 비교 중 nominal 후보 선택, cos bin, existing calibrated Psi2Flat_Trk, weight_pb를 고정했다. Eta/phi를 바꾼 뒤 다시 selection하거나 cos bin을 재배정하지 않았다.
- Nominal cos: fitted parent momentum + energy=sqrt(p²+stored nominal mass²), fitted slow momentum + fixed pion mass float32(0.13957018); parent rest frame으로 boost 후 axis=(-sin(Psi2Flat_Trk),cos(Psi2Flat_Trk),0)와 내적. 원래 cos 재현 차이는 최대4.55e-6 미만.
- Slow 성분 진단: D0Internal4-vector 고정. Original/fitted slow pion 및 fitted pT+original eta/phi, original pT+fitted eta/phi로 mass를 다시 계산했다. 이 조합은 대체 vertex fit이 아니다.
- Nominal stored mass-massDaugther1과 deltaMRefit은 다른 정의로 유지했다. diagDStarFit energy로 nominal boost를 조용히 대체하지 않았다.
- Candidate index는 모두0-based이다. 반드시 diagnostic_candidate_index를 사용해 주세요. Nominal candidate index와 같다는 보장이 없다.

#### 대표 후보

| Label | Run / lumi / event | Diagnostic candidate | before / nominal Delta-m [MeV] | Min normalized5x5 eigenvalue |
|---|---|---:|---:|---:|
| PR_target_entered_indefinite | 1 / 7084 / 459490357 | 10 | 145.066504 / 145.408977 | -0.646134 |
| PR_target_entered_PSD_control | 1 / 6168 / 400061033 | 12 | 144.637195 / 145.505960 | 0.331864 |
| PR_target_stayed_peak_indefinite | 1 / 9533 / 618328793 | 12 | 145.356544 / 145.346677 | -0.646144 |
| PR_target_left_peak_indefinite | 1 / 6335 / 410895098 | 12 | 145.298416 / 145.744311 | -0.646141 |
| PR_cos04_06_entered_indefinite | 1 / 6561 / 425542171 | 17 | 145.025212 / 145.318047 | -0.819484 |
| PR_cos08_10_entered_indefinite | 1 / 3179 / 206158633 | 0 | 145.040035 / 145.414322 | -0.712952 |
| NPR_target_entered_indefinite | 1 / 8708 / 141891265 | 26 | 143.589745 / 145.390389 | -0.646119 |
| NPR_target_entered_PSD_control | 1 / 3699 / 60270446 | 45 | 146.387305 / 145.307713 | 0.318743 |

#### 파일

- representative_candidates.json: 원본 diagInputFile LFN, diagnostic ROOT URL/tree/entry/index, event key, nominal cos 및 corrected EP, weight, 원본5x5 covariance와 각 fit state의 기대값.
- all_target_candidates.csv: 핵심 target903개 전체의 원본/진단 입력 및 event identity와 Delta-m 값.
- target_diagnostic_files.txt: target903개가 사용한 diagnostic ROOT 목록.
- verify_packet.py: 로컬 diagnostic ROOT에서 대표8개의 identity/covariance/state를 재검증하는 uproot 스크립트. CMSSW fit을 실행하는 스크립트가 아니다.
- slow_components.py 및 cos_migration.py: 분석 정의를 확인할 코드 사본. 재실행에는 기존 campaign npz 입력이 필요하다.
- production_agent_response_summary.md: 전달받은 production 검사와 로컬 재현 검사의 증거 범위 구분.

#### 주의

전달받은 보고서는 실제 production covariance 경로를 점검한7후보 중3개의 음의 고유값을 제시했지만, 그 후보들이 문제 cos 구간에 속하지 않는다고 명시했다. 따라서 현재 패킷으로 그 연결을 검증해야 한다. 이번 로컬 검사는 CMSSW 원본 fit 입력을 독립 재실행한 것이 아니다.

`nominal_candidate_selection.py` 및 `flat_selection_snapshot.h`는 기존 선택 및 preprocessing cut의 사본이다. CandidateSelection에 이미 적용된 daughter preselection을 포함해 재현할 때 참조해 주세요. Stored centrality0–20은 percent0–10에 해당한다.
<!-- source:SRC-41:end -->

<a id="src-42"></a>

## SRC-42 — handoff_replay_20260919/production_handoff/production_agent_response_summary.md

**기록 구분:** 조사 보고서·해석 (표본·시점별 결과). **원문:** [handoff_replay_20260919/production_handoff/production_agent_response_summary.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/handoff_replay_20260919/production_handoff/production_agent_response_summary.md). **SHA256:** `eedf360032649f47690dc1a75502d003137bb138c607c092f7e71e5bf101ad96`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-42:start -->
### Evidence update from the production-agent report supplied by Junseok

User-supplied report, not independently reproduced in this local turn:
- Seven candidates: saved original5x5 equals pseudoTrack, unpacked TrackRef, transient-track and initial curvilinear covariance element by element.
- Three listed candidates retain negative normalized eigenvalues after conversion to the actual pre-fit7x7. They are not candidates from the problem cosine cell.
- The rerun fitted states agree with the diagnostic ROOT for the tested candidates; no stale-state reuse path was identified.
- Common vertex constraints apply; no explicit D0/D* mass or PV constraint. The D0MassD0_sigma argument described in the report is supplied to the K/pi mass uncertainties.
- Nominal stored parent energy uses fitted daughter momentum with legacy/fixed masses; the retained nominal D0 daughter and final fitted D0 child are not the same stored state definition.

Verified locally for this handoff:
- The component-replacement study holds nominal cosine assignment and candidate selection fixed.
- The nominal boost uses stored nominal parent mass, not diagDStarFit energy; it reproduces nominal cosine within4.55e-6.
- The attached examples belong to the explicitly labeled problem and control cells; their ROOT identities/states are checked by verify_packet.py.

Inference boundary: the supplied production evidence supports that indefinite covariance can actually enter the tested fits; it does not establish that this caused the excess specifically in cos0.6–0.8, nor that a particular covariance repair is justified.
<!-- source:SRC-42:end -->

<a id="src-43"></a>

## SRC-43 — production_readonly_audit_20260929/BRANCH_SCHEMA.md

**기록 구분:** 정의·설계·재현 자료 (각 본문의 구현 상태 참조). **원문:** [production_readonly_audit_20260929/BRANCH_SCHEMA.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/BRANCH_SCHEMA.md). **SHA256:** `27c3ec40a9c6985ef0892271775f8463ca079b35df6b58323d4e8c5f3aac0b61`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-43:start -->
### 제안 branch 명세 — 구현하지 않음

현재 AFS data DStar tree에 추가할 **41개 top-level branch**. 배열 원소를 각각 branch로 세지 않는다. 기존 branch는 그대로 보존한다. D0 tree / custom eventplane tree / eventinfo tree에 diagEvent64를 각각 하나 추가하면 data 파일 전체 추가 branch object는 **44개**. 기존 RunNb/LSNb 및 EP/Q/weight 연결 정보는 재사용한다.

|#|그룹|이름|형식|정의|
|---:|---|---|---|---|
|1|identity|`diagEvent64`|uint64/event|Full EDM event number; existing RunNb/LSNb retained|
|2|identity|`diagInputFile`|string/event|Input LFN; not output file or just basename|
|3|identity|`diagCandidateIndex`|int64/candidate|Legacy accepted candidate index; -1 if not accepted|
|4|identity|`integrityD0Index`|uint32/candidate|Index in frozen first-D0 collection|
|5|identity|`diagTrackKey`|uint32[3]|Unpacked track keys, order K, pi, slow|
|6|identity|`diagTrackProductID`|uint32[3]|Track ProductID diagnostic; not global identity|
|7|identity|`diagPackedSource`|uint8[3]|0 packedPFCandidates, 1 lostTracks, 2 lostTracks:eleTracks, 255 unavailable|
|8|identity|`diagPackedKey`|uint32[3]|Key in source packed collection via existing unpacker track-to-packed Ptr vector|
|9|identity|`diagTrackReuse`|bool|Any same input track used in more than one daughter role|
|10|states|`diagKOriginal`|double[10]: px,py,pz,E,charge,ptError,mass,x,y,z|Original unpacked K TrackRef, before all fits|
|11|states|`diagPiOriginal`|double[10]: px,py,pz,E,charge,ptError,mass,x,y,z|Original unpacked pi TrackRef, before all fits|
|12|states|`diagSlowOriginal`|double[10]: px,py,pz,E,charge,ptError,mass,x,y,z|Original unpacked slow TrackRef, before all fits|
|13|states|`diagKFirst`|double[10]: px,py,pz,E,charge,ptError,mass,x,y,z|K child state after first D0 fit|
|14|states|`diagPiFirst`|double[10]: px,py,pz,E,charge,ptError,mass,x,y,z|pi child state after first D0 fit|
|15|states|`diagKInternal`|double[10]: px,py,pz,E,charge,ptError,mass,x,y,z|K child state after internal D0 refit in DStarFitter|
|16|states|`diagPiInternal`|double[10]: px,py,pz,E,charge,ptError,mass,x,y,z|pi child state after internal D0 refit in DStarFitter|
|17|states|`diagSlowInput`|double[10]: px,py,pz,E,charge,ptError,mass,x,y,z|Slow kinematic particle state immediately before DStar vertex fit, after transient-track state conversion|
|18|states|`diagSlowRefit`|double[10]: px,py,pz,E,charge,ptError,mass,x,y,z|Slow child state after final DStar fit|
|19|states|`diagD0First`|double[10]: px,py,pz,E,charge,ptError,mass,x,y,z|First D0 fit parent kinematic state (not rounded nominal candidate)|
|20|states|`diagD0Internal`|double[10]: px,py,pz,E,charge,ptError,mass,x,y,z|Internal D0 parent immediately before DStar vertex fit|
|21|states|`diagD0Refit`|double[10]: px,py,pz,E,charge,ptError,mass,x,y,z|D0 child after final DStar fit; do not label its K/pi children as newly refitted|
|22|states|`diagDStarFit`|double[10]: px,py,pz,E,charge,ptError,mass,x,y,z|Final DStar parent kinematic state|
|23|nominal_states|`diagD0OriginalP4`|double[4]: px,py,pz,E|Exact first stored D0 candidate p4|
|24|nominal_states|`diagLegacyD0P4`|double[4]: px,py,pz,E|Final fitted D0 momentum with first stored D0 mass, as used in nominal parent energy|
|25|nominal_states|`diagLegacySlowP4`|double[4]: px,py,pz,E|Exact nominal stored slow daughter p4|
|26|nominal_states|`diagLegacyDStarP4`|double[4]: px,py,pz,E|Exact nominal stored DStar parent p4; legacy boost reference|
|27|mass|`deltaMOriginal`|double, GeV|M(first stored D0 + original slow)-M(first stored D0)|
|28|mass|`deltaMRefit`|double, GeV|M(final D0 child + final slow child)-M(final D0 child)|
|29|mass|`diagDeltaMNominal`|double, GeV|Legacy nominal parent mass minus first stored D0 mass|
|30|mass|`deltaMDStar`|double, GeV|M(final children sum)-M(first stored D0 + original slow)|
|31|mass|`deltaMD0`|double, GeV|M(final D0 child)-M(first stored D0)|
|32|fit_status|`diagFitStatus`|uint16[3]|First D0, internal D0, final DStar stage; bits attempted/tree/state/vertex/daughters valid; not attempted distinct from failure|
|33|fit_status|`diagFitChi2`|double[3]|Raw fit chi2; retain negative/nonfinite values|
|34|fit_status|`diagFitNdf`|double[3]|Raw fit ndf|
|35|fit_status|`diagFitVertex`|double[3][3]|Vertex xyz in cm for each fit stage|
|36|fit_status|`diagTrackRefitMask`|uint8[3]|Per K,pi,slow role stage bitmask; no fictitious K/pi update at DStar stage|
|37|fit_status|`diagPassMask`|uint32|Legacy acceptance and named alternative acceptance/evaluated bits; proposal does not define or run alternative algorithm|
|38|covariance|`diagTrackOriginalCov`|double[3][25]|Row-major original 5x5, parameter order q/p,lambda,phi,dxy,dsz|
|39|covariance|`diagFitStateCov`|double[10][49]|Row-major 7x7 x,y,z,px,py,pz,m for non-original states in states list, including slow fit input|
|40|covariance|`diagVertexCov`|double[3][9]|Row-major 3x3 xyz covariance for each fit stage|
|41|covariance|`diagCovarianceValidity`|uint8[16]|3 original track + 10 fit state + 3 vertex matrices; unavailable/nonfinite/indefinite/PSD-singular/PD status, no matrix correction|

공통 단위: momentum/mass/energy GeV, position cm, 각도 rad. state 배열의 순서와 covariance 좌표 순서는 다르므로 schema metadata를 반드시 함께 저장한다. Fit-state ptError는 해당 covariance에서 전파한 값이며 original track ptError로 대체하지 않는다. Invalid/indefinite state도 기록하고 자동 보정하지 않는다.

Constraint 종류·mass hypothesis·mass sigma·fitter tolerance·GT/IOV·ONNX hash·feature 순서·code/config hash·EP 제외 후보 정의는 job 단위 metadata/TNamed/JSON으로 한 번 기록한다. 이들은 위 branch 수에 포함하지 않는다.

이 명세는 기존 성공 후보의 before/after 비교용이다. 실패/신규 유입까지 보려면 prefit pair diagnostic collection을 별도로 내보내야 한다. 독립적인 candidate-row PairDiagnostics tree로 만들 경우 위 41개 필드에 run/lumi 2개를 더한 43필드 구조를 쓸 수 있으며, 이것은 위 44개 branch 추가안과 별도 확장이다. 원래 prefit 선택을 통과하지 못한 조합까지 포함한다는 뜻은 아니다.

Cross-covariance, sigmaDeltaM, availability/status를 실제 구현해 저장할 때는 별도 3개 branch를 추가한다. 현재 주변 covariance만으로 독립 가정하여 sigmaDeltaM을 채우지 않는다. Core fitter의 CachingVertex 단계에서 track-to-track covariance를 노출해야 한다. 미구현 값을 available=true로 표시하지 않는다.

MC GEN 확장은 이 data 41개 계산에 포함하지 않는다. 기존 matchGEN을 보존하고 GEN index/PDG/p4/ancestor/matching deltaR/origin을 별도로 연결해야 한다. 기존 EOS full diagnostic patch의 GEN branches를 재사용할 수 있다.

선택 기준을 바꾸지 않는 진단 저장만으로도 D0Fitter와 DStarFitter에 상태 payload를 추가해야 한다. Producer 알고리즘 교체나 covariance 보정이 필요하다는 뜻은 아니다.
<!-- source:SRC-43:end -->

<a id="src-44"></a>

## SRC-44 — production_readonly_audit_20260929/REPORT.md

**기록 구분:** 조사 보고서·해석 (표본·시점별 결과). **원문:** [production_readonly_audit_20260929/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/REPORT.md). **SHA256:** `c5ca23644fc625ca40e2c68d730c86d125277f9abd645fb51f2024151c804dcc`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-44:start -->
### Data production read-only audit — 2026-09-29

#### 판단과 범위

**Refit 전후를 같은 후보에 저장하는 것은 가능하다. 실제 fit 처리를 바꾼다면 중심 변경 위치는 DStarFitter이며, 기존 nominal 결과를 보존한 진단 payload와 ntupler branch가 함께 필요하다. Data와 MC 모두 같은 MiniAOD→D 후보 reconstruction을 다시 실행해야 한다. GEN/SIM이나 detector RECO 전체를 다시 한다는 뜻은 아니다.**

이번에는 소스·PSet·ONNX·라이브러리 수정, build, cmsRun, job 제출을 하지 않았다. 이 디렉터리에 점검 보고서·명세·hash/설정 근거만 작성했다. 기존 소스에는 이번 작업 전부터 변경 사항이 있었으며 그것을 되돌리지 않았다.

검토 범위: AFS data Condor/canonical 설정과 archived CRAB PSet, MiniAOD unpacker, D0/DStar fitter와 producer, PAT6, custom tracker event-plane analyzer, official event-plane producer/flattening, EventInfoTreeProducer, 관련 CMSSW release fitter 내부. 코드상 실행 경로·상태 정의·실패 처리 점검이다. 실제 data ROOT의 모든 entry, calibration closure, covariance 분포, event overflow 발생률을 새로 검증한 결과는 아니다.

#### 1. 실제 설정을 어디까지 확인했는가

|대상|확인한 설정|증거|
|---|---|---|
|현재 AFS Condor data entry|D0 MVA > −1, D0 candidate-count filter 비활성, PAT6 EP on, EventInfo EP off|[PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:198](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:198); [PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:341](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:341); [PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:372](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:372)|
|현재 canonical data cfg|D0 MVA > 0.9, D0 candidate-count filter 활성, EventInfo EP on|[PbPb2023_D0BothAndDStar_MB_cfg_v2_Step2MVA.py:157](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/production/pbpb2023/data/PbPb2023_D0BothAndDStar_MB_cfg_v2_Step2MVA.py:157); [PbPb2023_D0BothAndDStar_MB_cfg_v2_Step2MVA.py:297](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/production/pbpb2023/data/PbPb2023_D0BothAndDStar_MB_cfg_v2_Step2MVA.py:297); [PbPb2023_D0BothAndDStar_MB_cfg_v2_Step2MVA.py:332](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/production/pbpb2023/data/PbPb2023_D0BothAndDStar_MB_cfg_v2_Step2MVA.py:332)|
|보관된 CRAB 09Mar26_v2 PSet|D0 MVA > 0.9, PAT6, DStar VtxChiProbCut=0, hiEvtPlaneRecalc/FlatRecalc. Module 원문과 줄 번호 별도 저장|[추출한 PSet](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/archived_crab_selected_modules.json)|
|Condor 제출 연결|submit_condor.sh data 모드는 위 Condor cfg를 선택. wrapper는 AFS CMSSW에서 실행|[submit_condor.sh:102](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/submission/condor/submit_condor.sh:102)|

현재 AFS의 D0Fitter, DStarFitter, PAT6, PATEventPlaneTrack, EventInfoTreeProducer, unpacker **6개 핵심 소스는 20260918 진단 작업 당시 보관한 baseline과 byte-identical**이다. [SHA 비교](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/baseline_comparison.json). 다만 특정 기존 data ROOT가 어느 job-time PSet/라이브러리로 만들어졌는지까지 이번에 연결한 것은 아니다. 해당 data output의 대표 파일/제출 manifest가 특정되지 않았으므로 현재 Condor 설정을 과거 data 전체의 확정 설정으로 부르지 않는다. CRAB request 이름의 CMSSW_13_2_13만으로 실제 release를 판정하지도 않는다.

공통 주요 설정:

- CMSSW work area 13_2_11, Run3_pp_on_PbPb_2023 era, data GT `132X_dataRun3_Prompt_v7`, 2023HI Golden JSON, HFtowers centrality table override. [PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:49](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:49), [PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:104](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:104), [PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:126](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:126).
- D0 K/pi pT > 1.5 GeV, |η| < 2.4, track relative pT error < 0.1, chi2 < 5, DCA < 0.05 cm, first-D0 flight significance > 3, first-D0 vertex probability threshold 0. [PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:172](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:172).
- Slow pT > 0.3 GeV, relative pT error < 0.1, track chi2 < 5, DStar pT > 4.5 GeV, vertex probability threshold 0. [PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:234](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:234). Slow η cut inherits cfi 999, not the D0 |η|<2.4 override; detector acceptance and explicit configured cut are different.
- ONNX `XGBoost_Model_OnlyNonPrompt_05Mar26_Centrality_pTerr_ptErr011_1.onnx`; 20 inputs in configured order. Production model remains unchanged.

#### 2. 호출 경로와 각 단계의 상태

1. `changeToMiniAOD` inserts `unpackedTracksAndVertices` and rewrites generalTracks/offlinePrimaryVertices InputTags to its outputs: [PATAlgos_cff.py:133](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/python/PATAlgos_cff.py:133).
2. Unpacker takes packedPFCandidates/lostTracks/eleTracks. For tracks with details it copies pseudoTrack reference point, momentum, charge, covariance; chi2/ndof are separately reconstructed. No positive-definite projection is applied: [TrackAndVertexUnpacker.cc:75](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/plugins/TrackAndVertexUnpacker.cc:75). It already outputs the track→packed Ptr vector, so stable input collection/key can be recorded **without changing the unpacker algorithm**: [TrackAndVertexUnpacker.cc:174](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/plugins/TrackAndVertexUnpacker.cc:174).
3. `D0Producer::produce → D0Fitter::fitAll`: original K/pi TransientTracks → first D0 common-vertex fit → first D0/children candidate → MVA → accepted D0 collection. [D0Fitter.cc:445](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/D0Fitter.cc:445).
4. `DStarProducer::produce → DStarFitter::fitAll`: frozen accepted D0 × slow-track pairs → prefit gates → **K/pi are refit again from original TrackRefs** → internal D0 composite + slow particle → DStar common-vertex fit. [DStarProducer.cc:48](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarProducer.cc:48), [DStarFitter.cc:597](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:597), [DStarFitter.cc:619](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:619).
5. Final fitted D0 and slow states update their pT/η/φ. K/pi are not independently refit again by this DStar fit. CMSSW `FinalTreeBuilder` replaces the D0 top particle and attaches its existing subtree; this is not a global three-track cascade smoothing: [FinalTreeBuilder.cc:105](/cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src/RecoVertex/KinematicFit/src/FinalTreeBuilder.cc:105).
6. Successful selected candidate is published; PAT6 copies its nominal values. Custom eventplane receives the DStar collection, while official hiEvtPlaneRecalc does not: [PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:300](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:300), [PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:341](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:341).

Both D0 fits and the DStar fit call `KinematicParticleVertexFitter`, with no explicit D0/DStar mass constraint and no PV/beamspot vertex constraint. Particle mass hypotheses with mass uncertainties are supplied; these are not a parent mass constraint. PV is used afterwards for impact/flight/pointing calculations and selection. The numerical seed covariance is a loose 10000 cm² diagonal, not a measured PV constraint: [KinematicParticleVertexFitter.cc:47](/cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src/RecoVertex/KinematicFit/src/KinematicParticleVertexFitter.cc:47).

|저장 대상|현재 nominal|전후 비교에 별도로 필요한 상태|
|---|---|---|
|DStar parent|Final parent momentum; energy from final D0 momentum + first stored D0 mass, and final slow momentum + fixed pion mass|Raw first-D0+slow p4; internal-D0+slow input; full final child p4 sum; final parent kinematic state|
|D0 daughter|First stored D0 candidate retained unchanged|Internal D0 before DStar fit and final D0 child|
|Slow daughter|Final fitted momentum and fixed pion-mass energy, but TrackRef remains original|Original track and actual prefit kinematic particle, plus final child|
|K/pi daughter|First-D0 stored daughters retained; original TrackRefs|Original, first fit, internal D0 refit; no invented final-DStar K/pi fit state|
|`pTerrD2` and daughter track qualities|Original TrackRef values, even when daughter momentum is fitted|Fit-state propagated pT error separately|

Code: [DStarFitter.cc:684](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:684), [DStarFitter.cc:788](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:788), [DStarFitter.cc:804](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:804), [PATCompositeTreeProducer6.cc:1508](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc:1508), [PATCompositeTreeProducer6.cc:1597](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc:1597), [PATCompositeTreeProducer6.cc:1635](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc:1635). Nominal's mixed definitions are confirmed; this by itself does not prove the central-bin cause. Nominal p4 construction also uses float energies, so exact legacy reproduction must preserve its arithmetic as well as the conceptual formula.

#### 3. 확인한 문제와 주의점

##### A. Fit reliability and prefit information

|항목|확인한 사실과 영향|근거|
|---|---|---|
|Valid does not imply PD covariance|Input PSD/eigenvalue gate is absent. CMSSW invertPosDefMatrix uses general inversion if Cholesky fails. An invertible indefinite matrix may therefore continue; valid alone cannot certify error estimates.|[invertPosDefMatrix.h:10](/cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src/DataFormats/Math/interface/invertPosDefMatrix.h:10); [KalmanVertexUpdator.cc:99](/cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src/RecoVertex/KalmanVertexFit/src/KalmanVertexUpdator.cc:99)|
|Negative χ² / nonfinite errors|D0 negative-chi2 veto is commented. Both probability thresholds are 0 and no independent χ²≥0/finite gate is present. sqrt/projected errors and division by length/ndf lack complete finite/positive checks. NaN comparisons can fail to reject. Actual occurrence rate in data is unmeasured.|[D0Fitter.cc:471](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/D0Fitter.cc:471); [D0Fitter.cc:544](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/D0Fitter.cc:544); [DStarFitter.cc:645](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:645); [DStarFitter.cc:752](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:752); [DStarFitter.cc:770](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:770)|
|Internal D0 is not an identical repeat of first fit|First fit mass σ: π=3.5e−7, K=1.6e−5 GeV. Internal refit passes 1.6e−4 GeV to **both** K/pi. This changes the fit input covariance/model; whether intentional and how much it affects Δm are not established here.|[D0Fitter.cc:57](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/D0Fitter.cc:57); [DStarFitter.cc:66](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:66); [DStarFitter.cc:601](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:601)|
|Raw option is not fit bypass|`useRawDStarKinematics` switches only parent p4 after valid fit. It still requires D0/DStar fits, fitted-vertex/PV extrapolation, topology gates; stored slow daughter remains fitted. Treating it as a no-fit reconstruction is incorrect.|[DStarFitter.cc:606](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:606); [DStarFitter.cc:696](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:696); [DStarFitter.cc:757](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:757); [DStarFitter.cc:788](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:788)|
|Duplicate daughter veto depends on debug|duplicateSlowPion is computed only inside debugCategoryCutflow, then used by rejectDuplicateSlowPion. With debug off it remains false. Current inspected configurations do not enable the veto; old unconditional veto is commented.|[DStarFitter.cc:443](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:443); [DStarFitter.cc:469](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:469); [DStarFitter.cc:481](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:481)|
|Missing failed combinations|Tree/state/vertex failures `continue` before any candidate is published. No stale-state fallback seen in this path, but absence from output hides failures.|[DStarFitter.cc:606](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:606); [DStarFitter.cc:623](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:623); [DStarFitter.cc:632](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:632); [DStarFitter.cc:639](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:639)|
|Existing prefit mass gate|Raw Δm > 0.160 GeV is rejected before fit. Current successful-output study is conditional on that original preselection; it cannot measure all possible raw↔fit migrations.|[DStarFitter.cc:510](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:510)|
|Numerical stopping criterion|Default transverse displacement 0.01 cm=100 μm, max 100 iterations. Termination is not a full 3D truth/pull test.|[KinematicParticleVertexFitter.cc:40](/cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src/RecoVertex/KinematicFit/src/KinematicParticleVertexFitter.cc:40); [SequentialVertexFitter.cc:272](/cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src/RecoVertex/VertexTools/src/SequentialVertexFitter.cc:272)|

The already completed [stability report](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/fit_stability_20260929/REPORT.md) found that tightening convergence did not remove the PR central concentration, with a few seed-dependent/invalid-statistics exceptions. Those are earlier tests; this audit did not rerun them. Neither covariance clipping nor stricter convergence is selected as a production fix here.

**Covariance coordinate distinction:** original track covariance is 5×5 `(q/p, λ, φ, dxy, dsz)`; a kinematic particle uses 7×7 `(x,y,z,px,py,pz,m)`. They cannot be compared element-by-element as the same matrix. Factory→TransientTrackKinematicStateBuilder uses the track impact-point free state; trajectory/coordinate propagation and a mass-error dimension are involved. The detailed-track unpacker copies the 5×5 covariance; it does not repair it. For `recoverTracks` with missing track details a separate fallback constructs `1e12 × I`, so those tracks must be distinguishable from true stored-track-detail inputs. [TrackAndVertexUnpacker.cc:101](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/plugins/TrackAndVertexUnpacker.cc:101); [KinematicParticleFactoryFromTransientTrack.cc:15](/cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src/RecoVertex/KinematicFitPrimitives/src/KinematicParticleFactoryFromTransientTrack.cc:15); [TransientTrackKinematicStateBuilder.cc:5](/cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src/RecoVertex/KinematicFitPrimitives/src/TransientTrackKinematicStateBuilder.cc:5).

##### B. Ntupler / event identity / bookkeeping

|항목|확인한 사실과 영향|근거|
|---|---|---|
|32-bit event number|PAT6, custom eventplane, EventInfo all store unsigned 32-bit EventNb. EDM event numbers are wider; values above 2³²−1 lose high bits. Actual affected event count has not been measured.|[PATCompositeTreeProducer6.cc:723](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc:723); [PATEventPlaneTrack.cc:851](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATEventPlaneTrack.cc:851); [EventInfoTreeProducer.cc:359](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/EventInfoTreeProducer.cc:359)|
|Reco candidate cap|PAT6 truncates at 50,000 with warning; custom EP accepts up to 500,000 then throws. If cap is reached, daughter exclusions can include candidates omitted from PAT6. No runtime frequency determined.|[PATCompositeTreeProducer6.cc:1271](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc:1271); [PATEventPlaneTrack.cc:72](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATEventPlaneTrack.cc:72); [PATEventPlaneTrack.cc:416](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATEventPlaneTrack.cc:416)|
|Original/fitted error mixing|Stored slow p4 is fitted but track pT error/impact quantities come from original TrackRef. Not a fitted covariance measurement.|[PATCompositeTreeProducer6.cc:1597](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc:1597); [DStarFitter.cc:793](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:793)|
|Parent DCA branch definition|PAT6 DStar-mode 3D DCA fields copy D0 daughter userFloat, while parent dca2D is computed from parent geometry. Do not silently interpret all DCA fields as one DStar fit stage.|[PATCompositeTreeProducer6.cc:1565](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc:1565)|
|Reset/failure review|Per-candidate reset exists; fit vectors cleared after each event in normal execution. No previous-candidate value reuse found on normal success/failure paths. However invalid candidate handle returns before some event scalar updates and analyze still fills: stale event fields are possible for misconfigured/missing collection, not demonstrated for valid production products.|[PATCompositeTreeProducer6.cc:379](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc:379); [PATCompositeTreeProducer6.cc:1802](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc:1802); [PATCompositeTreeProducer6.cc:2065](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc:2065); [DStarFitter.cc:908](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarFitter.cc:908)|
|GEN matching|Uses stored reco daughters (fitted slow, first stored D0/K/pi), accepts a passing GEN match; no one-to-one global assignment. Changing saved daughter kinematics can change matchGEN. Preserve old matching and compare new separately on MC.|[PATCompositeTreeProducer6.cc:1283](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc:1283); [PATCompositeTreeProducer6.cc:1403](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc:1403)|
|Unused MVA product label|Config MVAValuesNewDStar differs from producer MVAValuesDStar; current `useAnyMVA=false` and D0mva userFloat path avoid use of that label. Latent problem if later enabling this path, not a demonstrated current score error.|[PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:286](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:286); [DStarProducer.cc:33](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/DStarProducer.cc:33); [PATCompositeTreeProducer6.cc:1298](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc:1298)|

##### C. Event plane / event selection

1. **Tracker p/m branch names are reversed against the CMSSW catalog.** PAT6 eptrackpAngle/Q/SumW use indices 4/10 (=trackm2/3, η −2…−1); eptrackm uses 5/11 (=trackp2/3, η +1…+2). Raw/off angle naming follows the same reversal. [PATCompositeTreeProducer6.cc:525](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc:525), [HiEvtPlaneList.h:5](/cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src/RecoHI/HiEvtPlaneAlgos/interface/HiEvtPlaneList.h:5). Downstream symmetric combinations may be unchanged, but side-specific resolution/calibration interpretation must use actual contents. No downstream effect size measured here.
2. **EventInfo first HLT pattern does not match its implementation.** Condor/canonical first trigger string ends `_v*`; EventInfo uses C++ `find()` substring matching, not wildcard matching. Normal HLT names end `_v<number>` so first slot remains false and prescale −9. Other two configured names end `_v` and work as substrings. The actual hltHighLevel event filter handles its own patterns; this is an EventInfo bookkeeping issue, not proof the event path rejects every event. [PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:360](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:360), [EventInfoTreeProducer.cc:226](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/EventInfoTreeProducer.cc:226).
3. **Event filter flags are not all active event cuts.** colEvtSel is empty; Flag_colEvtSel effectively reflects HLT. Flag_hfCoincFilter is listed in EventInfo but not scheduled, so this slot remains false. primaryVertexFilter+clusterCompatibilityFilter run on a separate flag path, not as a prerequisite in DStarAna_step. An analysis that treats these flags as usual without checking the PSet can select incorrectly. [PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:390](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py:390), [EventInfoTreeProducer.cc:267](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/EventInfoTreeProducer.cc:267).
4. **Custom tracker EP changes with the set of reconstructed candidates.** It removes the union of charged daughters of all candidates in the configured mass range 1.79–2.25, using exact TrackRef ProductID/key. It does not use a geometric fallback to exclude extra nearby tracks. The track momenta used for Q are original unpacked tracks. Thus changing only fitted momentum for an identical candidate/track set does not rotate those tracks in Q; changing the accepted candidate set/mass-window membership can change which tracks are removed. [PATEventPlaneTrack.cc:326](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATEventPlaneTrack.cc:326), [PATEventPlaneTrack.cc:423](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATEventPlaneTrack.cc:423), [PATEventPlaneTrack.cc:658](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATEventPlaneTrack.cc:658).
5. Custom `trkQx/y` are **pT-normalized q**, not raw ΣpT·cos/sin, and are not flattened in this analyzer. Inclusive all_trkQ/W are also stored. Existing downstream calibration must stay tied to the same exclusion rule, track cuts, event key and weight. [PATEventPlaneTrack.cc:585](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATEventPlaneTrack.cc:585), [PATEventPlaneTrack.cc:758](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATEventPlaneTrack.cc:758). For the primary before/after mass/cos comparison use frozen legacy candidate set and legacy calibrated EP/weight; a newly recomputed EP belongs in a separate comparison.
6. Official hiEvtPlaneRecalc takes packed/lost tracks directly, no DStar input. In the packed path HF comes from packed candidates; setting caloTag=particleFlow does not force the alternate PF collection path. Tracks need details/highPurity and EPCuts. Both official producer and flat producer access slimmed PV[0] without an empty-vector guard; custom EP has PV/beamspot fallback instead. Empty-PV events are an unmeasured runtime risk, not evidence of current bad EP. [EvtPlaneProducer.cc:482](/cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src/RecoHI/HiEvtPlaneAlgos/src/EvtPlaneProducer.cc:482), [EvtPlaneProducer.cc:495](/cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src/RecoHI/HiEvtPlaneAlgos/src/EvtPlaneProducer.cc:495), [HiEvtPlaneFlatProducer.cc:197](/cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src/RecoHI/HiEvtPlaneAlgos/src/HiEvtPlaneFlatProducer.cc:197).
7. PAT6 reads only eventplaneSrcRecalc. Its flat/off/raw level indices agree with the release definitions; Q at level 2 is offset-corrected Q while the angle is separately flattened, so reconstructing every flat angle from saved sumCos/sumSin is not guaranteed. HF scalar SumW uses harmonic-3 indices; harmonic-specific SumWSub is the explicit choice. [PATCompositeTreeProducer6.cc:149](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc:149), [PATCompositeTreeProducer6.cc:581](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc:581), [HiEvtPlaneFlatProducer.cc:213](/cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src/RecoHI/HiEvtPlaneAlgos/src/HiEvtPlaneFlatProducer.cc:213).
8. Flat DB payload/IOV applicability to these Run3 track cuts is **not proven by this static audit**. EvtPlaneProducer can disable DB correction if LoadEPDB reports failure; FlatProducer does not examine IsSuccess. No new GT query or calibration closure was run. Current output should not be certified calibrated solely because a branch is named flat. [EvtPlaneProducer.cc:461](/cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src/RecoHI/HiEvtPlaneAlgos/src/EvtPlaneProducer.cc:461), [HiEvtPlaneFlatProducer.cc:178](/cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src/RecoHI/HiEvtPlaneAlgos/src/HiEvtPlaneFlatProducer.cc:178).

##### D. ONNX interface and job provenance

Configured inputs are pT, y, centrality, VtxProb, 3DCosPointingAngle, 3DPointingAngle, 2DCosPointingAngle, 2DPointingAngle, 3DDecayLength, 3DDecayLengthSignificance, 2DDecayLength, 2DDecayLengthSignificance, pTD1, EtaD1, pTerrD1, pTD2, EtaD2, pTerrD2, Trk3DDCA, dEta_dau. D1/D2 here are the **positive/negative track roles inside D0Fitter**, not universally K/pi species and not the DStar tree's D0/slow daughter numbering. pTerr inputs are original absolute track ptError; pT/η inputs are first-D0 fit momenta. centrality is the raw HFtowers bin. [D0Fitter.cc:360](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/D0Fitter.cc:360), [D0Fitter.cc:681](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/D0Fitter.cc:681).

No direct slow-pion/EP/cos input exists in this list. This does not rule out angular selection through correlations. Changing only the second DStar fit while keeping the first D0 fit and model fixed does not directly recalculate D0's score; it can still change selected candidates and mass/angle response. The mapping has an unknown-name→0 fallback and assumes output [1]; all 20 current names are recognized. This audit did not revalidate ONNX training feature semantics or efficiency in data. [D0Fitter.cc:703](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/D0Fitter.cc:703), [D0Fitter.cc:765](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/src/D0Fitter.cc:765).

Condor wrapper uses the live AFS release and helper log paths are fixed `logs/job.out/err/log`, not per-job filenames. Historical source/binary/config attribution needs the actual job-time snapshot; concurrent fixed log names can overwrite transfer outputs. This is provenance/operational risk, not the central mass mechanism. [submit_condor.sh:152](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/submission/condor/submit_condor.sh:152).

#### 4. 재생산 시 변경 범위 — 이번에는 수정하지 않음

|목적·위치|필요한 변경안|불필요하거나 미결정인 부분|
|---|---|---|
|전후 저장: D0Fitter|최초 fit state/covariance/vertex payload를 candidate에 추가|D0 selection/model/fit 변경은 불필요|
|전후 저장: DStarFitter|원본·internal D0·slow fit 입력·최종 state/metadata 추가; legacy 계산 유지|Covariance clipping이나 constraint 추가를 채택하지 않음|
|실제 DStar fit 교체/생략|pair loop, fit 호출, 후속 cut, parent/daughter 정의를 명시적으로 분기|useRawDStarKinematics만으로 불가능; 교체 알고리즘은 미결정|
|DStarProducer|기존 성공 candidate userdata는 copy/put으로 전달. 실패/신규 pair를 남기려면 별도 diagnostic collection 출력|모든 producer 알고리즘을 교체해야 한다는 뜻은 아님|
|PAT6 .cc/.h|Branch 선언, 초기화/reset, payload 읽기, 64-bit key 추가|기존 nominal branch 보존|
|Unpacker|기존 track→packed association으로 source/key 기록|Track covariance나 생성 알고리즘 변경 불필요|
|Event plane|비교 기준인 legacy 제외 후보 집합/Q/calibration 고정. 새 집합의 EP는 별도 비교. 각 tree에 event64 추가|Fit 변경 때문에 official EP 계산식까지 바꿀 필요는 없음|
|PSet/metadata|저장 모드, LFN, GT/IOV, code/library/ONNX hash, constraint/mass sigma 고정·기록|서로 다른 data cfg를 같은 production 설정으로 취급하면 안 됨|

기존 ROOT에 원래 track/state가 없으면 나중에 정확한 full refit 비교를 복구할 수 없다. 원본 MiniAOD가 있으면 그 입력으로 새 tree를 만들 수 있다. MiniAOD pseudoTrack의 정보량은 full RECO와 동일하지 않다. 저장된 진단 state를 사용하는 제한된 study와 전체 data/MC reconstruction을 구분한다.

#### 5. Branch 수와 tree 수의 구체안

**제안은 DStar tree에 41개, 나머지 기존 data 3개 tree에 event64를 각 1개씩, 총 44개 branch object 추가이다. 구현하지 않았다.** 하나의 구체적인 배열 schema이며 보편적인 최소 필요 개수는 아니다. [41개 전체 명세](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/BRANCH_SCHEMA.md) / [JSON 명세](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/proposed_data_branches.json).

|그룹|DStar 추가 branch 수|
|---|---:|
|event/LFN/candidate/D0/track 식별|9|
|K·pi·slow·D0·DStar의 13개 단계 상태: state당 10원소 배열|13|
|exact nominal/legacy p4 명시 저장|4|
|Δm before/after/nominal, δmDStar, δmD0|5|
|fit status/χ²/ndf/vertex, refit/pass flag|6|
|original track / fit-state / vertex covariance 및 validity|4|
|DStar 합계|41|

Fit-state covariance에는 **실제 slow fit 입력**도 포함한다. Original track covariance와 변환된 fit 입력을 구분한다. 중앙 이동만 보려면 처음 요청한 4개 scalar로도 가능하지만, 원인·vertex 신뢰성·후보 대응까지 점검하기에는 부족하다.

기존 DStar tree에 추가하므로 성공 후보의 전후 비교를 위해 같은 tree를 여러 벌 만들 필요는 없다. Fit 실패 후보와 새 방식에서만 채택되는 후보까지 추적하려면 prefit pair를 내보내는 별도 diagnostic collection/tree가 필요하다. 기존 성공 후보에 branch만 추가해서 원래 후보 집합 밖까지 복원할 수는 없다. 별도 PairDiagnostics tree의 43필드 설계는 명세에 구분해 두었다.

과거 EOS full diagnostic patch는 **299개 branch 추가** 설계다: 50개 기본 scalar + 12×12 state/cov/파생량 + 8개 배열 + 5×19 GEN + event64/LFN 2개. [기존 전체 목록](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/existing_diagnostic_branch_inventory.json). 이번 41개안은 data용으로 배열을 사용하고 중복 파생 scalar 및 GEN을 뺀 제안이다. 기존 299개가 이미 AFS에 반영됐다는 뜻은 아니다.

Cross-covariance/σ(Δm)는 41개안에 포함하지 않는다. 주변 covariance만으로 daughter들이 독립이라고 가정해 채우지 않는다. CMSSW CachingVertex에서 track-to-track covariance를 계산하므로 그 단계에서 꺼내는 구현은 검토할 수 있지만, 반환된 parent/daughter marginal만으로는 부족하다. 구현한다면 값 2개와 availability/status 1개, 총 3개 branch를 추가한다. 현재 unavailable 상태를 변경한 것은 아니다.

#### 6. 확인된 결론과 남은 한계

- 저장 설계와 변경 위치는 구체화했다. 기존 nominal을 유지하면서 동일 후보의 전후 값을 비교할 수 있다.
- 위 확인 사항 때문에 **fitter와 production 전체가 문제없다고 판단하지 않는다**. Indefinite covariance를 통과시키는 경로, internal mass-σ 차이, EP p/m 표기, EventInfo HLT/flag, event ID 폭은 각각 분리해서 변경 여부를 판단해야 한다.
- 이것들을 모두 중앙 peak 원인이라고 주장하지 않는다. 기존 보고서에서는 중앙 집중이 주로 vertex-fit 단계에 생겼고 nominal 정의의 추가 효과는 작았다. 전체 68% 폭은 증가했고 PSD에서도 중앙 유입이 있었다. 새 fit 방식의 채택은 아직 확정하지 않았다.
- 실제 production 전에 남은 검증은 대상 data의 job-time 설정과 대표 입력을 고정하고 legacy branch·후보·key·EP closure를 확인하는 일이다. 이번 요청은 check only이므로 새 runtime 검증은 실행하지 않았다. 정적 코드 점검 완료와 production 적합 판정은 구분한다.

#### 증거 파일

- [설정별 줄 번호와 SHA](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/configuration_evidence.json)
- [20260918 baseline과 비교](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/baseline_comparison.json)
- [점검 전 소스 SHA](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/source_hashes_before.json)
- [변경 없음 확인](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/read_only_verification.json)
- [구체적인 branch 명세](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/BRANCH_SCHEMA.md)

보고서의 source link는 현재 AFS/CVMFS 줄 번호이며, 과거 data ROOT의 job-time provenance를 직접 입증하는 링크는 아니다.
<!-- source:SRC-44:end -->

<a id="src-45"></a>

## SRC-45 — production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/REPORT.md

**기록 구분:** 조사 보고서·해석 (표본·시점별 결과). **원문:** [production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/REPORT.md). **SHA256:** `0cdf6af4dc5dbb05d5bc2abc4e90d4e4b3ce9ecb20c7dc860191bf0639a273b2`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-45:start -->
### Fix02 — 2023 PbPb data offline event selection

**수정 및 CMSSW configuration 검증 완료. 실제 event 처리와 production 제출은 수행하지 않았다.**

기존 상태: colEvtSel이 비어 있었고 HF flag path가 주석 처리됐다. PV+cluster는 별도 flag만 계산했으며 DStar production의 필수 cut이 아니었다.

공식 권고는 [CMS SWGuideHeavyIonCentrality](https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideHeavyIonCentrality#Event_selection)의 2023 PbPb 항목을 사용했다. 2024 HF 3Th5 설정과 구분했다. Beam-scraping은 이 세 조건에 추가하지 않았다.

|Filter|실제 입력·조건|
|---|---|
|primaryVertexFilter|offlineSlimmedPrimaryVertices; !isFake, abs(z)<=25 cm, rho<=2 cm|
|clusterCompatibilityFilter|hiClusterCompatibility; 공식 cfi와 동일: minZ -20, maxZ 20.05, clusterPars (0,0.0045), nhitsTrunc 150, clusterTrunc 2|
|phfCoincFilter2Th4|hiHFfilters:hiHFfilters; HF energy threshold 4 GeV에서 양쪽 각각 최소 2개|

PV 정의는 [공식 MiniAOD cff](https://raw.githubusercontent.com/CmsHI/cmssw/forest_CMSSW_13_2_X/HeavyIonsAnalysis/EventAnalysis/python/collisionEventSelection_cff.py), HF는 [공식 HiHFFilter 설정](https://raw.githubusercontent.com/CmsHI/cmssw/forest_CMSSW_13_2_X/HeavyIonsAnalysis/EventAnalysis/python/hffilter_cfi.py)을 대조했다. Slimmed PV의 비어 있는 track refs에 tracksSize 조건을 적용하지 않는다. 원래 unpacked PV에서 tracksSize>=2를 요구하던 정의와 구분한다.

#### 실행 경로 검증

##### condor

설정: [PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py)

```text
unpackedTracksAndVertices -> hltFilter -> primaryVertexFilter -> clusterCompatibilityFilter -> phfCoincFilter2Th4 -> generalD0CandidatesNew -> generalDStarCandidatesNew -> hiEvtPlaneRecalc -> hiEvtPlaneFlatRecalc -> d0ana_newreduced -> dStarana -> eventplane
```

개별 flag는 공통 HLT/unpacker 뒤에 각자 filter를 연결하므로 combined filter에 의해 먼저 차단되는 구조가 아니다.

- `Flag_colEvtSel`: unpackedTracksAndVertices → hltFilter → primaryVertexFilter → clusterCompatibilityFilter → phfCoincFilter2Th4
- `Flag_hfCoincFilter`: unpackedTracksAndVertices → hltFilter → phfCoincFilter2Th4
- `Flag_primaryVertexFilter`: unpackedTracksAndVertices → hltFilter → primaryVertexFilter
- `Flag_clusterCompatibilityFilter`: unpackedTracksAndVertices → hltFilter → clusterCompatibilityFilter

##### canonical

설정: [PbPb2023_D0BothAndDStar_MB_cfg_v2_Step2MVA.py](/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/production/pbpb2023/data/PbPb2023_D0BothAndDStar_MB_cfg_v2_Step2MVA.py)

```text
unpackedTracksAndVertices -> hltFilter -> primaryVertexFilter -> clusterCompatibilityFilter -> phfCoincFilter2Th4 -> generalD0CandidatesNew -> generalDStarCandidatesNew -> d0candCountFilter -> hiEvtPlaneRecalc -> hiEvtPlaneFlatRecalc -> d0ana_newreduced -> dStarana -> eventplane
```

개별 flag는 공통 HLT/unpacker 뒤에 각자 filter를 연결하므로 combined filter에 의해 먼저 차단되는 구조가 아니다.

- `Flag_colEvtSel`: unpackedTracksAndVertices → hltFilter → primaryVertexFilter → clusterCompatibilityFilter → phfCoincFilter2Th4
- `Flag_hfCoincFilter`: unpackedTracksAndVertices → hltFilter → phfCoincFilter2Th4
- `Flag_primaryVertexFilter`: unpackedTracksAndVertices → hltFilter → primaryVertexFilter
- `Flag_clusterCompatibilityFilter`: unpackedTracksAndVertices → hltFilter → clusterCompatibilityFilter

#### 기록 의미와 범위

- evtSel 배열은 3→4칸. [0]=combined, [1]=HF, [2]=PV, [3]=cluster. [2]의 과거 PV+cluster 결합 의미를 PV 단독으로 정리했다.
- EventInfo 첫 HLT pattern은 substring 검색에 맞춰 _v로 바꾸고 실제 HLT selector의 _v*는 유지했다.
- 이번 수정은 data Step2 두 진입 설정에 한정한다. MC/Step1/보관 CRAB snapshot은 변경하지 않았다. MC 효율과 GEN 분모 연결은 새 data 선택과 함께 재평가할 항목이다.
- 실제 data 이벤트의 통과율과 product availability를 runtime으로 새로 확인한 것은 아니다. Configuration load와 path wiring을 검증했다. 공식 MiniAOD event content가 HF/cluster summary를 보존함을 release 코드에서 확인했다.
- C++ build, cmsRun event processing, CRAB/Condor 제출은 하지 않았다. 기존 ROOT도 변경하지 않았다.

#### 변경 diff와 검증 산출물

- [두 설정 전체 diff](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/production_offline_selection.diff)
- [목록 MD 수정 diff](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/fixlist_update.diff)
- [모든 변경 diff](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/all_changes.diff)
- [설정 검증 log](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/config_validation.log)
- [공식 출처 목록](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/sources.json)
- [수정 전 hash](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/before_sha256.json)
- [전체 검증 요약](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/verification.json)
<!-- source:SRC-45:end -->

<a id="src-46"></a>

## SRC-46 — slow_gen_response_20260919/DEFINITIONS.md

**기록 구분:** 정의·설계·재현 자료 (각 본문의 구현 상태 참조). **원문:** [slow_gen_response_20260919/DEFINITIONS.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/slow_gen_response_20260919/DEFINITIONS.md). **SHA256:** `9dafa6fa0b05653ddc8c8e77c395b5d44af696ae4679c7720fdbe2ba2cebd5c9`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-46:start -->
### Slow-pion GEN response comparison

Frozen input: all 2,288 PR pT4 candidates, original nominal cos and weight, same five cosine bins. No new selection, covariance correction, refit or production change.

Use the exact same stored matched GEN slow-pion p4 for original TrackRef and fitted slow-pion states. Δη=ηreco−ηgen, Δφ=atan2(sin(φreco−φgen),cos(φreco−φgen)), ΔR=hypot(Δη,Δφ). Direction improvement means ΔRfit < ΔRoriginal. Relative pT residual=(pTreco/pTgen)−1. Weighted medians are empirical inverse CDF; RMS is about zero, not centered standard deviation. Means and Q16/Q84 are also saved. Percent errors group candidates by original input LFN/run/lumi/event.

Central transitions use the complete A→B definitions from the previous comparison: A=M(D0Internal+SlowOriginal)−M(D0Internal); B=M(D0Refit+SlowRefit)−M(D0Refit). Central interval [145.28813559322035,145.5084745762712) MeV. The labels entered, left, stayed, outside do not modify sample selection. PSD/indefinite classify only the original slow 5×5 covariance, normalized min eigenvalue threshold −1e−10.

Matching semantics: PATCompositeTreeProducer6.h:102–105 requires matching charge and ΔR<deltaR_; production PSet deltaR=0.03. processCandidates.cc (PATCompositeTreeProducer6.cc):1325–1326 calls this on the stored nominal/fitted slow daughter. D0 matching uses its stored daughter tracks, allowing both daughter permutations and handling photons in the daughter list. The first accepted GEN chain is stored (line1431 break), not a global nearest-match optimization. The photon-handling fallback is distinct from the Cholesky/general-inverse fallback in the fitter.

The saved matched GEN daughters come from one selected decay chain by construction. A consistent mother chain does not independently establish track truth or exclude alternative matches. diagIndependentTrackTruthAvailable records whether independent truth exists. No hit-based track truth or all alternative GEN matches are available in this extraction. Origin follows stored collisionId convention, not an independent detector truth classification.

A raw ΔR/charge check is evaluated against the same accepted GEN particle without rematching. Passing it does not prove a unique correct match. Fitted-selected samples can favor fitted agreement; this limitation must accompany any direction-improvement claim.

Duplicate multiplicities are only within the frozen selected sample. No rows are removed or reweighted. No claim is made about per-candidate inverse fallback beyond the previously replayed representatives.

GEN Δm is M(stored GEN DStar p4)−M(stored GEN D0 p4). Its before/after residuals and absolute-residual improvement are reported as a secondary check, conditional on the same accepted GEN match. This does not change central-bin boundaries.
<!-- source:SRC-46:end -->

<a id="src-47"></a>

## SRC-47 — slow_gen_response_20260919/INTERPRETATION.md

**기록 구분:** 조사 보고서·해석 (표본·시점별 결과). **원문:** [slow_gen_response_20260919/INTERPRETATION.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/slow_gen_response_20260919/INTERPRETATION.md). **SHA256:** `b27e7734a9b1b6ccf8d2a63f636d401f57148e5ed92ee55b552f2e45d1940878`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-47:start -->
#### 결과 해석과 다음 판단

PR pT4 2,288개 전체에서 slow 방향 ΔR이 fit 후 감소하는 가중 비율은 51.09%다. cos별 전체 비율은 51.65 / 48.04 / 53.56 / 51.93 / 50.36%로, 문제 구간 전체의 방향 개선 비율이 유독 높지는 않다.

그러나 cos 0.6–0.8 중앙 유입 56후보에서는 방향 개선 비율이 70.93 ± 6.12%다. PSD 17후보는 75.06 ± 10.80%, indefinite 39후보는 69.23 ± 7.39%다. 중앙 유출 32후보는 48.33 ± 8.97%다. 전체 cos 통합 중앙 유입 214후보에서도 67.14 ± 3.22%가 개선되므로, 중앙 유입과 GEN 방향 개선의 동반은 문제 구간에만 있는 현상이 아니다. 이 구분은 Δm 이동으로 사후 분류한 관측적 비교이며 인과 효과나 전체 resolution 개선의 증거가 아니다.

문제 구간 중앙 유입 후보의 ΔR 가중 중앙값은 0.002832 → 0.002035로 감소하지만 RMS는 0.003371 → 0.003629로 증가한다. 전체 문제 구간 RMS도 0.008454 → 0.008909로 증가한다. 다수 후보의 잔차 감소와 일부 큰 잔차/분포 폭 증가가 동시에 가능하다. 중앙 집중이 모두 GEN에서 멀어지는 이동이라는 설명은 이 자료와 맞지 않지만, fit 전체가 정상/더 정확하다고 결론낼 수도 없다.

GEN Δm 절대 잔차가 감소하는 비율은 문제 구간 중앙 유입에서 96.37%다. 해당 후보의 GEN Δm residual RMS는 0.318794 → 0.081886 MeV다. 이는 중앙 bin이 GEN Δm 근처에 있다는 조건과 밀접하므로 독립적인 fit 품질 검증으로 삼지 않는다. 전체 문제 구간의 GEN Δm residual RMS는 1.275176 → 1.322736 MeV로 커진다.

원본 slow 상태가 같은 accepted GEN에 대해 charge/ΔR<0.03 조건을 통과하지 못한 것은 28후보다. 이 28개는 모두 A/B 둘 다 중앙 밖인 outside이며, 문제 구간에는 그중 5개가 있다. 따라서 현재 선택 표본의 중앙 유입은 original→fit matching 경계 통과 때문은 아니다. 단, 원본 표본에 없는 unmatched 후보나 다른 GEN 선택 가능성은 검증하지 않았다. fitted 기반 matching 선택의 영향 전체를 배제할 수 없다.

동일 slow track의 반복 후보는 선택된 2,288개 내에서 0개다. 동일 GEN D*를 공유하는 후보는 145개 GEN group에 걸쳐 292개이며, 문제 구간 중앙 유입 56개 중 4개가 이 범주다. 모든 그룹에서 후보를 유지했다. 서로 다른 slow track이 같은 GEN decay에 연결될 수 있음을 보여주며, 매칭의 유일성/정확성은 별도 문제다. 모든 후보의 stored independent truth flag는 0이므로 mother chain 일치만으로 진짜 daughter track임을 증명하지 않는다.

현재 권고: covariance clipping, 후보 제거 또는 production 변경을 바로 적용하지 않는다. 다음으로 독립 확인이 필요한 대상은 (1) 같은 GEN D*에 연결된 서로 다른 slow track들, (2) 중앙 유입 중 GEN 방향에서 더 멀어진 후보와 큰 ΔR 악화 후보다. 필요한 테스트는 원본 MiniAOD에서 해당 이벤트의 전체 GEN 후보에 대한 matching 대안과 실제 fit 직전 covariance/역산 경로를 대조하는 것이다. 기존 8대표에서 fallback이 확인되었다는 사실을 전체 indefinite 후보의 fallback 여부로 대체하지 않는다. 이번 결과는 저장 ROOT 비교이며 새 fit replay나 matching 변경을 하지 않았다.
<!-- source:SRC-47:end -->

<a id="src-48"></a>

## SRC-48 — slow_gen_response_20260919/REPORT.md

**기록 구분:** 조사 보고서·해석 (표본·시점별 결과). **원문:** [slow_gen_response_20260919/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/slow_gen_response_20260919/REPORT.md). **SHA256:** `28c52e274f1c8cf96d73ae25e0821ffb2d973346b87d59058365bee31343baf2`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-48:start -->
### PR pT4 slow-pion GEN response

#### 결과 해석과 다음 판단

PR pT4 2,288개 전체에서 slow 방향 ΔR이 fit 후 감소하는 가중 비율은 51.09%다. cos별 전체 비율은 51.65 / 48.04 / 53.56 / 51.93 / 50.36%로, 문제 구간 전체의 방향 개선 비율이 유독 높지는 않다.

그러나 cos 0.6–0.8 중앙 유입 56후보에서는 방향 개선 비율이 70.93 ± 6.12%다. PSD 17후보는 75.06 ± 10.80%, indefinite 39후보는 69.23 ± 7.39%다. 중앙 유출 32후보는 48.33 ± 8.97%다. 전체 cos 통합 중앙 유입 214후보에서도 67.14 ± 3.22%가 개선되므로, 중앙 유입과 GEN 방향 개선의 동반은 문제 구간에만 있는 현상이 아니다. 이 구분은 Δm 이동으로 사후 분류한 관측적 비교이며 인과 효과나 전체 resolution 개선의 증거가 아니다.

문제 구간 중앙 유입 후보의 ΔR 가중 중앙값은 0.002832 → 0.002035로 감소하지만 RMS는 0.003371 → 0.003629로 증가한다. 전체 문제 구간 RMS도 0.008454 → 0.008909로 증가한다. 다수 후보의 잔차 감소와 일부 큰 잔차/분포 폭 증가가 동시에 가능하다. 중앙 집중이 모두 GEN에서 멀어지는 이동이라는 설명은 이 자료와 맞지 않지만, fit 전체가 정상/더 정확하다고 결론낼 수도 없다.

GEN Δm 절대 잔차가 감소하는 비율은 문제 구간 중앙 유입에서 96.37%다. 해당 후보의 GEN Δm residual RMS는 0.318794 → 0.081886 MeV다. 이는 중앙 bin이 GEN Δm 근처에 있다는 조건과 밀접하므로 독립적인 fit 품질 검증으로 삼지 않는다. 전체 문제 구간의 GEN Δm residual RMS는 1.275176 → 1.322736 MeV로 커진다.

원본 slow 상태가 같은 accepted GEN에 대해 charge/ΔR<0.03 조건을 통과하지 못한 것은 28후보다. 이 28개는 모두 A/B 둘 다 중앙 밖인 outside이며, 문제 구간에는 그중 5개가 있다. 따라서 현재 선택 표본의 중앙 유입은 original→fit matching 경계 통과 때문은 아니다. 단, 원본 표본에 없는 unmatched 후보나 다른 GEN 선택 가능성은 검증하지 않았다. fitted 기반 matching 선택의 영향 전체를 배제할 수 없다.

동일 slow track의 반복 후보는 선택된 2,288개 내에서 0개다. 동일 GEN D*를 공유하는 후보는 145개 GEN group에 걸쳐 292개이며, 문제 구간 중앙 유입 56개 중 4개가 이 범주다. 모든 그룹에서 후보를 유지했다. 서로 다른 slow track이 같은 GEN decay에 연결될 수 있음을 보여주며, 매칭의 유일성/정확성은 별도 문제다. 모든 후보의 stored independent truth flag는 0이므로 mother chain 일치만으로 진짜 daughter track임을 증명하지 않는다.

현재 권고: covariance clipping, 후보 제거 또는 production 변경을 바로 적용하지 않는다. 다음으로 독립 확인이 필요한 대상은 (1) 같은 GEN D*에 연결된 서로 다른 slow track들, (2) 중앙 유입 중 GEN 방향에서 더 멀어진 후보와 큰 ΔR 악화 후보다. 필요한 테스트는 원본 MiniAOD에서 해당 이벤트의 전체 GEN 후보에 대한 matching 대안과 실제 fit 직전 covariance/역산 경로를 대조하는 것이다. 기존 8대표에서 fallback이 확인되었다는 사실을 전체 indefinite 후보의 fallback 여부로 대체하지 않는다. 이번 결과는 저장 ROOT 비교이며 새 fit replay나 matching 변경을 하지 않았다.


전체 2288후보, frozen nominal cos/weight/selection. PSD/indefinite는 원본 slow covariance만 분류한다. 중앙 이동은 완전한 D0Internal+original slow → 최종 D0Refit+fitted slow 기준이다.

#### GEN 연결 및 검증

- 모든 후보에서 기존 matchGEN 및 diagGenSlowMatched를 확인했다. 원본/fit ΔR 재계산과 저장값 최대 차이는 3.83e-16 / 4.07e-08.
- 같은 GEN에 대한 original matching 조건(charge, ΔR<0.03) 실패: 28개. Fitted 조건 실패: 0개.
- Mother chain checks: {'slow_mother_matches_dstar': 2288, 'd0_mother_matches_dstar': 2288, 'k_mother_matches_d0': 2288, 'pi_mother_matches_d0': 2288}. 이는 저장된 한 GEN decay 내부 일관성 검사이며 독립 track truth 검증이 아니다.
- GEN slow origin: {'1': 2288}; independent track truth flags: {'0': 2288}.
- 선택 표본 내 동일 slow track 반복 후보: 0개; 동일 GEN D* 반복 후보: 292개. 후보를 제거하지 않았다.

#### 전체 구간별 방향 response

개선=동일 GEN slow pion에 대한 ΔR이 fit 후 감소. 비율과 중앙값은 기존 weight 사용. 오차는 event 단위로 묶은 표준오차. ΔR은 η–φ 거리이며 각도 rad 자체가 아니다.

|cos bin|group|N|방향 개선 %|ΔR median original → fit|ΔR RMS original → fit|pT 잔차 개선 %|
|---|---|---:|---:|---|---|---:|
|0.0–0.2|all|456|51.65 ± 2.35|0.003614 → 0.003195|0.008859 → 0.008943|53.42|
|0.0–0.2|PSD|169|54.49 ± 3.85|0.003554 → 0.003038|0.009028 → 0.008642|55.69|
|0.0–0.2|indefinite|287|49.99 ± 2.94|0.003705 → 0.003342|0.008759 → 0.009115|52.09|
|0.2–0.4|all|462|48.04 ± 2.33|0.003347 → 0.003052|0.010150 → 0.008298|47.80|
|0.2–0.4|PSD|166|48.18 ± 3.90|0.003549 → 0.002905|0.013227 → 0.008456|46.33|
|0.2–0.4|indefinite|296|47.96 ± 2.91|0.003327 → 0.003142|0.007914 → 0.008208|48.63|
|0.4–0.6|all|424|53.56 ± 2.43|0.003595 → 0.003246|0.009086 → 0.009049|51.42|
|0.4–0.6|PSD|148|55.41 ± 4.09|0.003520 → 0.002979|0.008105 → 0.007815|53.38|
|0.4–0.6|indefinite|276|52.56 ± 3.00|0.003760 → 0.003476|0.009577 → 0.009654|50.35|
|0.6–0.8|all|501|51.93 ± 2.24|0.003425 → 0.003045|0.008454 → 0.008909|51.51|
|0.6–0.8|PSD|171|54.46 ± 3.85|0.003495 → 0.003025|0.009389 → 0.009512|56.78|
|0.6–0.8|indefinite|330|50.61 ± 2.77|0.003421 → 0.003113|0.007926 → 0.008579|48.77|
|0.8–1.0|all|445|50.36 ± 2.42|0.003527 → 0.003166|0.009228 → 0.009562|47.83|
|0.8–1.0|PSD|158|55.77 ± 4.03|0.003479 → 0.003508|0.008280 → 0.008883|48.72|
|0.8–1.0|indefinite|287|47.34 ± 3.02|0.003659 → 0.002973|0.009718 → 0.009921|47.34|

#### 전체 cos 통합 중앙 이동별 비교

|group|transition|N|방향 개선 %|ΔR median original → fit|ΔR RMS original → fit|
|---|---|---:|---:|---|---|
|all|entered|214|67.14 ± 3.22|0.002912 → 0.002019|0.004244 → 0.004277|
|all|left|165|51.86 ± 3.92|0.002713 → 0.002310|0.004690 → 0.004834|
|all|stayed|184|59.00 ± 3.63|0.002347 → 0.001774|0.003603 → 0.003742|
|all|outside|1725|48.15 ± 1.21|0.003949 → 0.003908|0.010282 → 0.010021|
|PSD|entered|66|78.47 ± 5.10|0.002953 → 0.001884|0.003905 → 0.003592|
|PSD|left|55|50.03 ± 6.80|0.002639 → 0.002209|0.004058 → 0.004035|
|PSD|stayed|66|57.58 ± 6.08|0.002568 → 0.001884|0.003350 → 0.003874|
|PSD|outside|625|50.89 ± 2.02|0.003866 → 0.003685|0.011007 → 0.009691|
|indefinite|entered|148|62.16 ± 3.99|0.002832 → 0.002076|0.004385 → 0.004546|
|indefinite|left|110|52.78 ± 4.80|0.002726 → 0.002334|0.004976 → 0.005188|
|indefinite|stayed|118|59.81 ± 4.53|0.002290 → 0.001705|0.003739 → 0.003665|
|indefinite|outside|1100|46.59 ± 1.51|0.003991 → 0.004094|0.009844 → 0.010206|

#### 문제 cos 0.6–0.8 중앙 이동별 비교

|group|transition|N|방향 개선 %|ΔR median original → fit|ΔR RMS original → fit|
|---|---|---:|---:|---|---|
|all|entered|56|70.93 ± 6.12|0.002832 → 0.002035|0.003371 → 0.003629|
|all|left|32|48.33 ± 8.97|0.002134 → 0.002410|0.003067 → 0.003912|
|all|stayed|44|65.91 ± 7.15|0.002290 → 0.001445|0.002986 → 0.004100|
|all|outside|369|47.68 ± 2.61|0.004024 → 0.003924|0.009662 → 0.010116|
|PSD|entered|17|75.06 ± 10.80|0.002632 → 0.001570|0.002863 → 0.002978|
|PSD|left|10|50.00 ± 15.81|0.002134 → 0.002367|0.002903 → 0.002501|
|PSD|stayed|15|73.33 ± 11.42|0.002137 → 0.001382|0.002797 → 0.005661|
|PSD|outside|129|50.01 ± 4.45|0.004279 → 0.003829|0.010668 → 0.010683|
|indefinite|entered|39|69.23 ± 7.39|0.002955 → 0.002111|0.003558 → 0.003864|
|indefinite|left|22|47.54 ± 10.88|0.002755 → 0.002422|0.003142 → 0.004428|
|indefinite|stayed|29|62.07 ± 9.01|0.002422 → 0.002048|0.003079 → 0.002988|
|indefinite|outside|240|46.42 ± 3.24|0.003927 → 0.004124|0.009072 → 0.009796|

#### 해석의 범위

이 표본은 fitted slow pion을 이용한 geometric GEN matching을 이미 통과한 후보이다. 따라서 방향 개선 비율만으로 matching 정확성이나 fit의 무편향성을 증명하지 않는다. Original 상태도 같은 GEN에 대해 ΔR/charge 조건을 통과하는지 함께 보고하며, 이 검사 역시 매칭의 유일성이나 hit truth를 증명하지 않는다. 저장된 GEN chain의 mother 일치는 matching이 올바른 decay를 선택했다는 독립 증거가 아니다.

모든 GEN 후보를 대상으로 rematching하거나 실패한 후보를 복원하지 않았다. 원본 선택에 없는 후보의 matching 유입률은 이 파일로 알 수 없다. 전체 후보별 Cholesky/general-inverse fallback을 계측하지 않았고 covariance 그룹을 fallback flag로 사용하지 않는다.

candidate_response.csv/json에는 후보별 signed Δη, wrapped Δφ, ΔR, relative pT residual, 이동량, 개선 flag, nominal cos/weight, GEN index 및 중복 multiplicity가 있다. response_metrics.csv/json은 모든 5 cos × covariance × transition 조합 및 전체 cos 통합 집계다. 방향 scatter는 candidate 점을 동일 크기로 표시하며, 표의 통계만 가중치가 적용된다.

Reproduction: bash run.sh, then python3 analyze_gen.py and python3 report.py in the LCG_106 environment with TMPDIR and MPLCONFIGDIR set to this analysis work directory.
<!-- source:SRC-48:end -->

<a id="src-49"></a>

## SRC-49 — three_stage_pr4_20260919/DEFINITIONS.md

**기록 구분:** 정의·설계·재현 자료 (각 본문의 구현 상태 참조). **원문:** [three_stage_pr4_20260919/DEFINITIONS.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/three_stage_pr4_20260919/DEFINITIONS.md). **SHA256:** `aa2ea0dc08c6cab7f637547983d4aee2494f098d175edcc9d78bca9692985987`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-49:start -->
### PR pT4 three-stage comparison contract

The supplied manifest currently contains only PR 501 candidates in nominal |cos theta*EP| 0.6–0.8 (NPR rows are not included in this PR-first study). The other four full cos-bin populations are missing. No new selection or cos reconstruction is substituted.

Stage A (before D* vertex fit): M(diagD0Internal.p4 + diagSlowOriginal.p4) - M(diagD0Internal.p4).
Stage B (final fitted children): M(diagD0Refit.p4 + diagSlowRefit.p4) - M(diagD0Refit.p4).
Stage C (nominal): integrityDmLegacy, the exact legacy producer definition. Do not replace it with the rounded float branch subtraction.

All mass calculations use complete stored four-vectors. No partial pT/eta/phi replacement is used. Original slow state is the original TrackRef state; the factory's tiny numerical momentum/coordinate conversion is not separately persisted for every candidate.

Use manifest nominal_cos_signed, weight_pb, and diagnostic_candidate_index unchanged. Bin edges in absolute nominal cos: 0, 0.2, 0.4, 0.6, 0.8, 1; include cos=1 in last bin. Central mass bin is [0.14528813559322035,0.1455084745762712) GeV. No mass-window cut is applied by this comparison; plotting range is 0.140–0.153 GeV, and under/overflow counts are reported.

Groups: normalized original slow-track 5x5 covariance eigenvalue < -1e-10 -> indefinite; otherwise PSD within numerical tolerance. This does NOT classify every daughter/D0 input covariance. Nonfinite matrices or nonpositive diagonal entries stop the analysis for explicit inspection rather than being silently removed.

Central 68% width: weighted empirical inverse-CDF Q84-Q16 using the complete fixed sample, not only plotted masses or central-bin candidates. Both endpoints and weighted median are saved. The quoted peak-fraction and paired peak-change errors use event-cluster influence sums, sqrt(sum_event(sum_candidate w*(indicator-mean))^2)/sum(w). Widths are point estimates, with no significance claim.

Transitions are evaluated for A->B, B->C, A->C: entered, left, stayed inside, stayed outside; save unweighted counts and weighted fractions. All state extraction failures must be resolved before reporting a full supplied-sample result.

Missing full-bin input: a CSV with the same columns as production_handoff/all_target_candidates.csv, containing all selected PR pT4 candidates in all 5 cos bins. Diagnostic ROOT URL/tree/entry/candidate index, event identity, frozen nominal cos, and weight_pb are essential; diagInputFile and existing audit columns are retained for checks. Existing source CSV can be expanded without redefining the selection.

I/O note: the KNU endpoint returns nonnumeric readv limit-query responses. Extraction limits vector-read requests to 64 elements of at most 1 MiB when that numeric query cannot be parsed. This only changes I/O batching; ROOT payload values and physics definitions are unchanged. Authenticated proxy and EOS working directories are used.
<!-- source:SRC-49:end -->

<a id="src-50"></a>

## SRC-50 — three_stage_pr4_20260919/REPORT.md

**기록 구분:** 조사 보고서·해석 (표본·시점별 결과). **원문:** [three_stage_pr4_20260919/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/three_stage_pr4_20260919/REPORT.md). **SHA256:** `9185b9fa34560c326b7e1c591e24872b1f7feb85b8dfa951187619a90fdddf16`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-50:start -->
### PR pT4 완전한 상태의 세 단계 Δm 비교 — 문제 cos bin 완료, 나머지 4개 bin 자료 필요

#### 범위와 핵심 결과

동일 후보·weight·nominal cos bin을 고정하여 **제공된 PR pT4 501후보 전체**, 382개 기존 diagnostic ROOT를 읽었다. 중복 candidate identity는 없으며 event는 495개다. 5개 원격 파일의 timeout은 재시도하여 모두 회수했고 누락·후보 제외 없이 계산했다. ROOT event/candidate index, nominal Δm, reco pT/y 및 원본 covariance 고유값을 전달 CSV와 대조했다.

**현재 완료한 범위는 |cosθ*EP| 0.6–0.8뿐이다.** 전달 패키지의 all_target_candidates.csv는 PR501+NPR402=903개의 이 구간 후보만 포함한다. 다른 cos 구간의 대표 2후보를 전체 구간 표본처럼 사용하지 않았다. 요청한 5-bin 전체 비교는 나머지 4개 구간의 frozen candidate manifest가 없어 미완료다.

이 501후보에서 중앙 bin의 가중 비중은 fit 전 **15.1522%**, 최종 fitted children **19.9985%**, nominal **20.2004%**다. Vertex-fit 전후에 +4.8463 percentage points, nominal 정의에서 추가 +0.2019 points가 나타난다. 이 후보 집합에서는 중앙 비중 증가 대부분이 일관된 최종 fitted children 단계에서 이미 나타나며, nominal 저장 정의의 추가 영향은 중앙 유입 1후보다.

반면 가중 central 68% 폭 Q84−Q16은 **1.29897 → 1.44465 → 1.44616 MeV**로 증가한다. 중앙 bin이 커지는 현상과 전체 central interval이 좁아지는 현상은 같지 않다. 이번 표본에서는 중앙 증가와 폭 증가가 함께 나타난다. 이 결과를 전체 cos 의존성이나 covariance의 인과 효과로 확장하지 않는다.

#### 고정한 정의

A (fit 전): `M(diagD0Internal.p4 + diagSlowOriginal.p4) - M(diagD0Internal.p4)`.
B (일관된 최종 fitted children): `M(diagD0Refit.p4 + diagSlowRefit.p4) - M(diagD0Refit.p4)`.
C (nominal): 저장된 `integrityDmLegacy`.

세 단계 모두 완전한 상태로 계산했다. η/φ 또는 pT 부분 복원을 하지 않았다. A의 original slow는 저장된 original TrackRef state이며 factory의 미세한 변환 후 state를 501후보에서 따로 재생산한 것은 아니다. B와 저장 deltaMRefit의 최대 차이는 6.67e-15 GeV, A와 전달 before의 최대 차이는 4.04e-10 GeV이며 central-bin membership에 영향이 없었다.

이 B는 이전 'D0Internal을 고정하고 fitted slow만 사용'한 19.80% 정의와 다르다. 이번에는 D0도 최종 fitted state로 바뀌므로 두 비율을 같은 계산으로 취급하지 않는다.

선택은 기존 reco pT7–10 GeV, |y|<0.3, centrality0–10%와 전달된 nominal selection을 그대로 따른다. weight_pb 및 nominal_cos_signed를 변경하지 않았고, cos를 재계산하거나 bin을 재배정하지 않았다. 중앙 Δm bin은 [0.14528813559322035,0.1455084745762712) GeV이며 판정에는 반올림 전 값을 사용한다.

PSD/indefinite는 **원본 slow track의 5×5 covariance**를 대각 정규화한 최소 고유값 기준이다. <−1e-10이면 indefinite, 나머지는 수치 허용오차 내 PSD다. D0/K/π를 포함한 전체 fit 입력의 정상성을 판정하는 그룹명이 아니다. 두 그룹 모두 그대로 보존했다.

#### 중앙 bin 가중 비중

오차는 동일 event를 묶은 influence-sum 표준오차이며, 단계 간 비교에는 paired 오차를 별도로 계산했다.

|그룹|후보 / event|A fit 전 %|B fitted children %|C nominal %|
|---|---|---:|---:|---:|
|all|501 / 495|15.1522 ± 1.6061|19.9985 ± 1.7971|20.2004 ± 1.8039|
|PSD|171 / 170|14.7864 ± 2.7326|18.3570 ± 2.9798|18.3570 ± 2.9798|
|indefinite|330 / 330|15.3418 ± 1.9949|20.8495 ± 2.2493|21.1561 ± 2.2614|

#### 가중 central 68% 폭 (MeV)

전체 고정 후보에서 empirical weighted inverse-CDF Q84−Q16을 사용했다. 중심 bin 또는 그림 범위로 자르지 않았다. 폭은 점추정치이며 CI/유의도를 제시한 것이 아니다. Q16/median/Q84는 stage_metrics.csv에 있다.

|그룹|A fit 전|B fitted children|C nominal|
|---|---:|---:|---:|
|all|1.298967|1.444655|1.446160|
|PSD|1.392886|1.420523|1.420468|
|indefinite|1.279306|1.477035|1.478864|

#### 중앙 유입·유출

유입/유출은 unweighted 후보 수이며, 순변화는 기존 weight를 사용한 percentage points다. 두 수치를 혼동하지 않는다.

|그룹|단계|유입|유출|중앙 비중 순변화 pp (paired event error)|
|---|---|---:|---:|---:|
|all|before → fitted_children|56|32|+4.8463 ± 1.8612|
|all|fitted_children → nominal|1|0|+0.2019 ± 0.2017|
|PSD|before → fitted_children|17|10|+3.5706 ± 3.0037|
|PSD|fitted_children → nominal|0|0|+0.0000 ± 0.0000|
|indefinite|before → fitted_children|39|22|+5.5077 ± 2.3553|
|indefinite|fitted_children → nominal|1|0|+0.3066 ± 0.3061|

두 그룹 모두 B에서 중앙 비중이 증가했다. Indefinite 그룹의 증가가 수치상 더 크지만 이 표만으로 두 그룹의 차이가 covariance 때문이라고 결론내리지 않는다. 원본 후보 선택·kinematics·covariance 그룹의 차이가 함께 존재한다.

#### 그림 및 산출물

- three_stage_distributions.png / .pdf: PSD와 indefinite별 A/B/C overlay. 파랑 실선=A, 주황 점선=B, 초록 점선=C. 거의 겹치는 B/C도 선 모양으로 구분했다. 회색은 중앙 bin.
- stage_metrics.csv/json: 중앙 비중, Q16/median/Q84/68% 폭, underflow/overflow 및 n/event/weight sum.
- central_transitions.csv/json: A→B, B→C, A→C의 유입/유출/잔류/바깥잔류, count·weight·fraction·paired error.
- paired_candidates.csv: 후보별 A/B/C Δm, covariance 그룹, frozen nominal cos·weight 및 재현 identity.
- candidates.json: ROOT에서 읽은 완전한 네 state, 원본 covariance 및 원래 CSV metadata. 이 파일만 있으면 그림/통계를 다시 계산할 수 있다.
- validation.json, extraction_status.json, provenance.json: 원본 대조, coverage 및 추출 성공 여부.
- extract.py, analyze.py, run.sh: 재실행 코드. 먼저 cache를 확인한다. 후보 manifest가 바뀌면 해당 ROOT group은 다시 읽는다.

히스토그램 범위는 140–153 MeV이며 모든 단계/그룹에서 범위 밖 후보는 0개다. 그래도 width 계산은 이 plotting range에 제한하지 않았다.

#### 아직 필요한 입력

나머지 |cos| [0,0.2), [0.2,0.4), [0.4,0.6), [0.8,1]의 **전체 PR pT4 frozen selected-candidate 목록**이 필요하다. 가장 간단한 전달 형식은 기존 all_target_candidates.csv를 5개 bin 전체로 확장한 CSV다. 동일한 diagnostic ROOT URL/tree/entry/index, run/lumi/event, nominal_cos_signed, weight_pb 및 검증용 열을 유지하면 현재 코드로 바로 처리할 수 있다. 데이터 없는 bin을 0으로 표시하거나 원래 선택/EP/weight를 임의로 다시 만들지 않았다.

Covariance clipping, 후보 제거, constraint 변경, production 재생산은 수행하지 않았다. 이 작업은 기존 저장 ROOT에 대한 상태 비교뿐이다.
<!-- source:SRC-50:end -->

<a id="src-51"></a>

## SRC-51 — three_stage_pr4_all_cos_20260919/DEFINITIONS.md

**기록 구분:** 정의·설계·재현 자료 (각 본문의 구현 상태 참조). **원문:** [three_stage_pr4_all_cos_20260919/DEFINITIONS.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/three_stage_pr4_all_cos_20260919/DEFINITIONS.md). **SHA256:** `e8c60d1e05badf5815d020dd5ab44d6d5f54ba8e2744672bc478c0320a30bf38`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-51:start -->
### PR pT4 three-stage comparison contract

The supplied all-cos manifest contains all 2,288 PR pT4 candidates in the five frozen nominal cosine bins. All 824 requested ROOT files were read. No new selection or cos reconstruction is substituted.

Stage A (before D* vertex fit): M(diagD0Internal.p4 + diagSlowOriginal.p4) - M(diagD0Internal.p4).
Stage B (final fitted children): M(diagD0Refit.p4 + diagSlowRefit.p4) - M(diagD0Refit.p4).
Stage C (nominal): integrityDmLegacy, the exact legacy producer definition. Do not replace it with the rounded float branch subtraction.

All mass calculations use complete stored four-vectors. No partial pT/eta/phi replacement is used. Original slow state is the original TrackRef state; the factory's tiny numerical momentum/coordinate conversion is not separately persisted for every candidate.

Use manifest nominal_cos_signed, weight_pb, and diagnostic_candidate_index unchanged. Bin edges in absolute nominal cos: 0, 0.2, 0.4, 0.6, 0.8, 1; include cos=1 in last bin. Central mass bin is [0.14528813559322035,0.1455084745762712) GeV. No mass-window cut is applied by this comparison; plotting range is 0.140–0.153 GeV, and under/overflow counts are reported.

Groups: normalized original slow-track 5x5 covariance eigenvalue < -1e-10 -> indefinite; otherwise PSD within numerical tolerance. This does NOT classify every daughter/D0 input covariance. Nonfinite matrices or nonpositive diagonal entries stop the analysis for explicit inspection rather than being silently removed.

Central 68% width: weighted empirical inverse-CDF Q84-Q16 using the complete fixed sample, not only plotted masses or central-bin candidates. Both endpoints and weighted median are saved. The quoted peak-fraction and paired peak-change errors use event-cluster influence sums, sqrt(sum_event(sum_candidate w*(indicator-mean))^2)/sum(w). Widths are point estimates, with no significance claim.

Transitions are evaluated for A->B, B->C, A->C: entered, left, stayed inside, stayed outside; save unweighted counts and weighted fractions. All state extraction failures must be resolved before reporting a full supplied-sample result.

I/O note: the KNU endpoint returns nonnumeric readv limit-query responses. Extraction limits vector-read requests to 64 elements of at most 1 MiB when that numeric query cannot be parsed. This only changes I/O batching; ROOT payload values and physics definitions are unchanged. Authenticated proxy and EOS working directories are used.
<!-- source:SRC-51:end -->

<a id="src-52"></a>

## SRC-52 — three_stage_pr4_all_cos_20260919/INTERPRETATION.md

**기록 구분:** 조사 보고서·해석 (표본·시점별 결과). **원문:** [three_stage_pr4_all_cos_20260919/INTERPRETATION.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/three_stage_pr4_all_cos_20260919/INTERPRETATION.md). **SHA256:** `7e41fd46312cd7767a7db177a4577131363499aa738662881a1e2b4458030ea6`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-52:start -->
#### 해석

PR pT4 2,288개 전체 비교에서 중앙 비중 증가량은 cos 0.6–0.8이 +4.846%p로 가장 크다. 나머지 네 구간의 가중 통합 증가량은 +1.525%p이고, 둘의 차이는 +3.321 ± 2.094%p(동일 event 연관성 반영)다. 따라서 수치상 최대라는 사실은 확인되지만 해당 구간에서 유독 크다는 통계적 결론은 아직 충분하지 않다. PSD에서 0.4–0.6, 0.6–0.8, 0.8–1.0의 증가는 각각 +3.378, +3.571, +3.204%p로 비슷하다. Indefinite에서는 0.6–0.8 증가가 가장 크지만, 다른 구간 통합과의 차이는 +3.563 ± 2.660%p다.

전체 후보 기준 중앙 68% 폭은 다섯 구간 모두 fit 후 증가한다. PSD 하위표본의 일부 구간은 폭이 감소하므로 모든 하위표본이 넓어진다고 해석하면 안 된다. Nominal 정의의 중앙 비중 추가 변화는 구간별 약 −0.23~+0.48%p로, 문제 구간의 주된 증가를 설명하지 않는다. 중앙 한 bin의 비중 증가는 전체 resolution 개선을 뜻하지 않는다. 이 비교만으로 covariance 이상이 집중의 원인이라는 인과관계는 확정할 수 없다.

<!-- source:SRC-52:end -->

<a id="src-53"></a>

## SRC-53 — three_stage_pr4_all_cos_20260919/NEXT_STEPS.md

**기록 구분:** 계획·제안 원문 (완료의 증거 아님). **원문:** [three_stage_pr4_all_cos_20260919/NEXT_STEPS.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/three_stage_pr4_all_cos_20260919/NEXT_STEPS.md). **SHA256:** `68e1bcf9180842333a13047033a089cc4e68cd2a17edb27ec98372a7fa570aa7`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-53:start -->
#### 다음 대조 테스트

현재 자료로는 0.6–0.8 특이 효과나 indefinite covariance의 인과 효과가 확정되지 않았다. 다음 우선순위는 원래 production을 유지한 채 후보별 GEN daughter 연결을 확인하고, slow-pion의 fit 전후 방향 잔차를 GEN 방향에 대해 비교하는 것이다. 중앙 진입·이탈·잔류를 나누고 각 cos/covariance 그룹에서 같은 nominal 선택과 weight를 유지한다. Δφ는 wrapping하고, 각도 잔차와 Δm 이동을 함께 대조한다. Fit 후 GEN 잔차가 줄었는지/커졌는지를 먼저 확인해야 정상적인 response와 잘못된 이동을 구분할 단서가 생긴다.

이 2,288개 추출에는 GEN daughter p4/매칭 ΔR와 후보별 실제 역산 fallback 기록을 추가로 읽지 않았다. 따라서 전체 indefinite 후보가 모두 fallback을 탔다거나 matching이 정상이라는 결론을 내리지 않는다. Fallback과의 직접 연결은 중앙 유입·유출 및 대조 후보를 실제 fit 경로에서 추가 계측해야 한다. 기존 대표 8후보 결과를 전체 표본 발생률로 확장하지 않는다. 임의 covariance clipping이나 후보 제거를 먼저 시행하지 않는다.
<!-- source:SRC-53:end -->

<a id="src-54"></a>

## SRC-54 — three_stage_pr4_all_cos_20260919/REPORT.md

**기록 구분:** 조사 보고서·해석 (표본·시점별 결과). **원문:** [three_stage_pr4_all_cos_20260919/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/three_stage_pr4_all_cos_20260919/REPORT.md). **SHA256:** `d90796169d742305499eca762078a598d72353943de6b3089a58adc7b99f460d`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-54:start -->
### PR pT4: five nominal cos bins, three complete states

All 2,288 candidates from 824 diagnostic ROOT files were read. Nominal cos, weights and candidate selection are fixed. The previous 501 candidate CSV rows and all their stage metrics reproduce within numerical precision. No covariance repair, removal, new fits or production changes.

A = M(diagD0Internal + diagSlowOriginal) − M(diagD0Internal). B = M(diagD0Refit + diagSlowRefit) − M(diagD0Refit). C = integrityDmLegacy. All use complete stored states; original slow is the saved TrackRef state.

PSD/indefinite classify ONLY the original slow 5×5 covariance after diagonal normalization; indefinite means minimum eigenvalue < −1e−10. Width = weighted empirical Q84−Q16, without mass truncation. Central bin = [145.28813559322035,145.5084745762712) MeV.

#### 해석

PR pT4 2,288개 전체 비교에서 중앙 비중 증가량은 cos 0.6–0.8이 +4.846%p로 가장 크다. 나머지 네 구간의 가중 통합 증가량은 +1.525%p이고, 둘의 차이는 +3.321 ± 2.094%p(동일 event 연관성 반영)다. 따라서 수치상 최대라는 사실은 확인되지만 해당 구간에서 유독 크다는 통계적 결론은 아직 충분하지 않다. PSD에서 0.4–0.6, 0.6–0.8, 0.8–1.0의 증가는 각각 +3.378, +3.571, +3.204%p로 비슷하다. Indefinite에서는 0.6–0.8 증가가 가장 크지만, 다른 구간 통합과의 차이는 +3.563 ± 2.660%p다.

전체 후보 기준 중앙 68% 폭은 다섯 구간 모두 fit 후 증가한다. PSD 하위표본의 일부 구간은 폭이 감소하므로 모든 하위표본이 넓어진다고 해석하면 안 된다. Nominal 정의의 중앙 비중 추가 변화는 구간별 약 −0.23~+0.48%p로, 문제 구간의 주된 증가를 설명하지 않는다. 중앙 한 bin의 비중 증가는 전체 resolution 개선을 뜻하지 않는다. 이 비교만으로 covariance 이상이 집중의 원인이라는 인과관계는 확정할 수 없다.

#### all

Peak fractions and widths use frozen weights. Entry/exit counts are unweighted. Change errors are paired event-cluster influence standard errors.

| cos bin | N | Peak A/B/C (%) | A→B change (pp) | A→B enter/leave | B→C enter/leave | Width A/B/C (MeV) |
|---|---:|---|---|---|---|---|
| 0.0–0.2 | 456 | 16.111 / 15.007 / 15.007 | -1.103 ± 1.695 | 27/32 | 1/1 | 1.281 / 1.426 / 1.426 |
| 0.2–0.4 | 462 | 16.449 / 18.194 / 18.194 | +1.746 ± 1.959 | 44/37 | 1/1 | 1.196 / 1.292 / 1.297 |
| 0.4–0.6 | 424 | 14.051 / 16.670 / 17.146 | +2.618 ± 1.885 | 37/26 | 2/0 | 1.287 / 1.355 / 1.358 |
| 0.6–0.8 | 501 | 15.152 / 19.999 / 20.200 | +4.846 ± 1.861 | 56/32 | 1/0 | 1.299 / 1.445 / 1.446 |
| 0.8–1.0 | 445 | 14.479 / 17.456 / 17.227 | +2.977 ± 2.137 | 50/38 | 1/2 | 1.281 / 1.495 / 1.511 |

#### PSD

Peak fractions and widths use frozen weights. Entry/exit counts are unweighted. Change errors are paired event-cluster influence standard errors.

| cos bin | N | Peak A/B/C (%) | A→B change (pp) | A→B enter/leave | B→C enter/leave | Width A/B/C (MeV) |
|---|---:|---|---|---|---|---|
| 0.0–0.2 | 169 | 17.358 / 13.168 / 13.168 | -4.190 ± 2.589 | 6/13 | 0/0 | 1.314 / 1.480 / 1.530 |
| 0.2–0.4 | 166 | 13.434 / 14.631 / 14.631 | +1.197 ± 2.985 | 13/12 | 0/0 | 1.491 / 1.484 / 1.485 |
| 0.4–0.6 | 148 | 16.216 / 19.595 / 20.270 | +3.378 ± 3.229 | 14/9 | 1/0 | 1.377 / 1.290 / 1.288 |
| 0.6–0.8 | 171 | 14.786 / 18.357 / 18.357 | +3.571 ± 3.004 | 17/10 | 0/0 | 1.393 / 1.421 / 1.420 |
| 0.8–1.0 | 158 | 12.814 / 16.018 / 15.377 | +3.204 ± 3.320 | 16/11 | 1/2 | 1.349 / 1.752 / 1.748 |

#### indefinite

Peak fractions and widths use frozen weights. Entry/exit counts are unweighted. Change errors are paired event-cluster influence standard errors.

| cos bin | N | Peak A/B/C (%) | A→B change (pp) | A→B enter/leave | B→C enter/leave | Width A/B/C (MeV) |
|---|---:|---|---|---|---|---|
| 0.0–0.2 | 287 | 15.383 / 16.082 / 16.082 | +0.699 ± 2.211 | 21/19 | 1/1 | 1.187 / 1.295 / 1.295 |
| 0.2–0.4 | 296 | 18.142 / 20.195 / 20.195 | +2.054 ± 2.559 | 31/25 | 1/1 | 1.109 / 1.178 / 1.176 |
| 0.4–0.6 | 276 | 12.874 / 15.079 / 15.446 | +2.205 ± 2.320 | 23/17 | 1/0 | 1.247 / 1.424 / 1.422 |
| 0.6–0.8 | 330 | 15.342 / 20.849 / 21.156 | +5.508 ± 2.355 | 39/22 | 1/0 | 1.279 / 1.477 / 1.479 |
| 0.8–1.0 | 287 | 15.409 / 18.260 / 18.260 | +2.851 ± 2.767 | 34/27 | 0/0 | 1.186 / 1.431 / 1.471 |

#### Target-bin increase versus other bins

Differences account for shared events across cos bins. These are descriptive asymptotic errors, not a corrected discovery test. Widths are point estimates.

| Group | Comparison | Target minus other increase (pp) | Standardized difference |
|---|---|---:|---:|
| all | 0 | +5.950 ± 2.517 | 2.36 |
| all | 1 | +3.101 ± 2.701 | 1.15 |
| all | 2 | +2.228 ± 2.648 | 0.84 |
| all | 4 | +1.869 ± 2.834 | 0.66 |
| all | other_four | +3.321 ± 2.094 | 1.59 |
| PSD | 0 | +7.760 ± 3.965 | 1.96 |
| PSD | 1 | +2.374 ± 4.235 | 0.56 |
| PSD | 2 | +0.192 ± 4.413 | 0.04 |
| PSD | 4 | +0.367 ± 4.477 | 0.08 |
| PSD | other_four | +2.789 ± 3.366 | 0.83 |
| indefinite | 0 | +4.808 ± 3.230 | 1.49 |
| indefinite | 1 | +3.454 ± 3.476 | 0.99 |
| indefinite | 2 | +3.303 ± 3.305 | 1.00 |
| indefinite | 4 | +2.657 ± 3.634 | 0.73 |
| indefinite | other_four | +3.563 ± 2.660 | 1.34 |

Comparison indices 0/1/2/4 correspond to [0,.2)/[.2,.4)/[.4,.6)/[.8,1]; other_four pools all four non-target bins using their original weights. The samples are observational: differences do not establish a covariance cause.

Detailed rows: stage_metrics.csv, central_transitions.csv, paired_candidates.csv. Reproduction: run.sh production_handoff_all_cos/pr4_all_cos_candidates.csv, then summarize.py in the same LCG environment.

#### 다음 대조 테스트

현재 자료로는 0.6–0.8 특이 효과나 indefinite covariance의 인과 효과가 확정되지 않았다. 다음 우선순위는 원래 production을 유지한 채 후보별 GEN daughter 연결을 확인하고, slow-pion의 fit 전후 방향 잔차를 GEN 방향에 대해 비교하는 것이다. 중앙 진입·이탈·잔류를 나누고 각 cos/covariance 그룹에서 같은 nominal 선택과 weight를 유지한다. Δφ는 wrapping하고, 각도 잔차와 Δm 이동을 함께 대조한다. Fit 후 GEN 잔차가 줄었는지/커졌는지를 먼저 확인해야 정상적인 response와 잘못된 이동을 구분할 단서가 생긴다.

이 2,288개 추출에는 GEN daughter p4/매칭 ΔR와 후보별 실제 역산 fallback 기록을 추가로 읽지 않았다. 따라서 전체 indefinite 후보가 모두 fallback을 탔다거나 matching이 정상이라는 결론을 내리지 않는다. Fallback과의 직접 연결은 중앙 유입·유출 및 대조 후보를 실제 fit 경로에서 추가 계측해야 한다. 기존 대표 8후보 결과를 전체 표본 발생률로 확장하지 않는다. 임의 covariance clipping이나 후보 제거를 먼저 시행하지 않는다.
<!-- source:SRC-54:end -->

<a id="src-55"></a>

## SRC-55 — three_stage_pr4_all_cos_20260919/STATUS.md

**기록 구분:** 해당 시점 상태·제출 기록 (현재 queue 조회 아님). **원문:** [three_stage_pr4_all_cos_20260919/STATUS.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/three_stage_pr4_all_cos_20260919/STATUS.md). **SHA256:** `d719fb6e0f1bb84f6c22288f041011775b1df185dde36dc43da5530df2c848b0`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-55:start -->
Complete: 2,288 / 2,288 candidates, 824 / 824 ROOT files, no failures. All five cos bins compared for before / fitted children / nominal states and PSD / indefinite groups. See REPORT.md and completion_validation.json. No covariance repair, candidate removal or production modification.
<!-- source:SRC-55:end -->

<a id="src-56"></a>

## SRC-56 — three_stage_pr4_all_cos_20260919/production_handoff_all_cos/README.md

**기록 구분:** 정의·설계·재현 자료 (각 본문의 구현 상태 참조). **원문:** [three_stage_pr4_all_cos_20260919/production_handoff_all_cos/README.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/three_stage_pr4_all_cos_20260919/production_handoff_all_cos/README.md). **SHA256:** `3cb62b0c928375b66f13a825e628528b583d82aaa392b8029b518d6c7b3983f5`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-56:start -->
PR pT4, reco 7–10 GeV, |y|<0.3, centrality 0–10%, all five nominal cosine bins.
Same schema as production_handoff/all_target_candidates.csv. Nominal cosine, event weights and EP remain fixed. 2288/2288 PR candidates; target 501 rows exactly match prior delivery.
Use diagnostic_root, diagInputFile, event key and diagnostic_candidate_index for lookup. nominal_candidate_index can differ.
Requested comparison: before vertex fit, consistent final fitted children, nominal stored definition, split by original slow covariance PSD/indefinite. No covariance repair or candidate removal.
<!-- source:SRC-56:end -->

<a id="src-57"></a>

## SRC-57 — vertex_truth_validation_20260929/REPORT.md

**기록 구분:** 조사 보고서·해석 (표본·시점별 결과). **원문:** [vertex_truth_validation_20260929/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/vertex_truth_validation_20260929/REPORT.md). **SHA256:** `9d46066d3e3b329a3ecb96acbf1350e39d18aa923bbfc4a876c9c4c7a797f876`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-57:start -->
### D* vertex position and uncertainty: 64-candidate pilot, 2026-09-29

#### 결론과 범위

기존 official MiniAOD와 저장된 재현 fit 상태를 이용해 64후보를 검사했다. PR/NPR 각각 original BDT pass/fail에서 candidate index의 SHA256 순서로 16개씩 뽑았다. Vertex 잔차나 Δm 결과로 후보를 선택하지 않았다. 모두 문제 tracker-axis |cosθ*| 0.6–0.8 표본이며, 5개 cos 구간 전체를 재검사한 결과가 아니다. 기존 weight, candidate identity, BDT group을 유지했다. 64개 모두 읽기·event/GEN p4 대조·reference screen을 통과했다.

**조건부 관측:** 일부 valid fit은 GEN 기준점 추정치에서 수 mm 이상 벗어나며 예측된 비행방향 오차보다 큰 잔차를 보인다. PR pass의 한 후보는 −10.81 mm / 1.64 mm = −6.59 pull, NPR pass의 한 후보는 +4.46 mm / 0.86 mm = +5.18 pull이다. 작은 pilot이므로 전체 covariance calibration이나 PR/NPR/BDT 차이를 확정하지 않는다. 기존 kinematic GEN matching을 사용했으며 독립적인 hit-level association으로 잘못된 track matching 가능성을 배제하지 않았다.

**Truth 수준:** 원래 official event의 VtxSmeared/HepMC가 없으므로 event별 직접 truth 검증은 아직 아니다. 독립적으로 보존된 full-chain benchmark에서 검증한 background GEN 기준점 추출 방법을 원래 event로 옮겨 적용했다. 아래 coverage는 이 전이 가정에 조건부인 pilot 수치다. Reco PV를 truth로 사용하지 않았다.

#### 좌표계 대조

보존된 Prompt_5_1, Prompt_100_11, Nonprompt_100_21의 GEN-SIM과 MiniAOD를 run/lumi/event로 연결했다. GEN-SIM의 generatorSmeared HepMC는 실제 VtxSmeared 출력의 복사본이다. MiniAOD signal D*/D0 및 daughter와 PDG ID/4-vector가 유일하게 일치하는 HepMC 입자의 production vertex를 대조했다. HepMC mm를 cm로 변환했다. 한 event의 여러 signal 입자에 동일한 translation이 적용됐는지도 확인했다.

- Full GEN background 기준점을 사용하는 대조: 205 event 중 197 event에서 연결 가능. 나머지는 signal matching ambiguity로 제외한 검증 event이며 로그에 남았다. 최대 기준점 차이 4.31e−7 cm = 4.31 nm.
- MiniAOD 기준점 대조: 191 event 연결 가능. Background가 남지 않은 6 event와 signal matching ambiguity 8 event는 로그에 남았다.
- 단순 background mode는 4개 background 입자만 남은 한 event에서 1.00 μm 틀렸다. 첫 background/root 입자도 다른 event에서 3.45 μm 틀려 보편적인 대안이 아니었다.
- 명확한 mode의 필요성을 반영해 mode count >=10, fraction >0.5, unique mode라는 경험적 screen을 적용했다. MiniAOD 검증 169 event에서 최대 차이 4.31 nm. 이 screen은 이번 benchmark를 보고 정한 것이므로 독립적인 모든-event 보증으로 해석하지 않는다.
- Official pilot 64개는 모두 screen을 통과하며, 각 event의 motherless background vertex도 하나였다. 실제 기준점은 collisionId=1 입자의 modal vertex. D* decay 기준점은 이 translation + matched slow-pion의 stored GEN production vertex로 정했다. 따라서 NPR의 B flight displacement를 유지했다.

Benchmark와 official의 저장내용은 완전히 동일하지 않으며 pruned background multiplicity도 다를 수 있다. Benchmark의 nm 수준 일치는 그 검증 표본의 수치이며 original official event별 정확도 보증이 아니다. 원래 event의 smeared HepMC 또는 SimVertex가 확보되면 직접 대조해야 한다.

#### 정의

n = matched GEN D0 momentum unit vector. Residual r = fitted D* vertex − translated matched slow-pion GEN production vertex. Longitudinal residual = n·r, sigma = sqrt(n^T C_vertex n). 두 transverse 축은 n과 beam axis로 만든 직교기저를 사용했다. Units: source positions cm, covariance cm²; report residual/sigma mm. Reported pull is a vertex-position residual divided by predicted vertex sigma; it is not the refitted-minus-original track-state change divided by an input sigma.

All 64 fitted vertex 3x3 covariances are positive definite. Original slow-track covariance validity is retained separately; positive vertex covariance does not certify the full fit. No clipping, candidate removal, new fits, BDT training, production modification or Condor submissions were performed.

Gaussian nominal 1D coverage is about 68.3% within ±1 sigma and 95.4% within ±2 sigma; real selection, nonlinearities and tails can change the observed distribution. Pull std below is the weighted population standard deviation about its weighted mean. Each pilot subgroup has N=Neff=16, so one candidate changes coverage by 6.25 percentage points. No significance claim is made.

#### 비행방향 결과

|Group|N|Median sigma (mm)|Residual median (mm)|Residual Q84−Q16 (mm)|Pull mean|Pull std|±1σ coverage|±2σ coverage|
|---|---:|---:|---:|---:|---:|---:|---:|---:|
|PR_pass|16|1.646|0.037|5.948|-0.264|1.966|62.50%|81.25%|
|PR_fail|16|1.777|-0.237|3.256|-0.021|0.451|100.00%|100.00%|
|NPR_pass|16|1.559|-0.187|3.345|0.195|1.552|68.75%|93.75%|
|NPR_fail|16|1.595|-1.113|4.680|-0.283|1.068|43.75%|100.00%|

Pass = original MVA >=0.95; fail = 0<MVA<0.95. BDT>0 inclusive is the union, not this fail category alone. N=16 per group is insufficient to establish a BDT-specific uncertainty defect. The PR-pass pull std is strongly affected by a single large negative tail. PR-fail's narrow pull sample likewise does not establish overestimated uncertainty in the full population. Transverse metrics and 3D Mahalanobis coverage are in CSV; cross-axis correlations are retained in the 3D calculation.

#### 큰 잔차의 대표 후보

|Index|Group|run:lumi:event|Residual (mm)|sigma (mm)|pull|Propagation Δm (MeV)|Constraint Δm (MeV)|
|---|---|---|---:|---:|---:|---:|---:|
|3148|PR_pass|1:4629:300264116|-10.813|1.641|-6.588|2.572|-0.113|
|10270|NPR_pass|1:9836:160279480|4.457|0.860|5.184|1.631|0.052|

Both have valid fitted vertices and positive fitted vertex covariances; vertex chi2/ndf are 1.539/1 and 4.264/1 respectively. Both original slow-track covariances are indefinite. Their background modes contain 48/50 and 49/49 retained background particles, respectively. This shows why valid/chi2 alone cannot establish vertex accuracy. It does not establish covariance as the unique cause, and these tail candidates do not by themselves explain the central peak.

Signed D0 flight is negative in 16/64 pilot candidates (PR pass/fail 5/3, NPR pass/fail 3/5). This is the projection from the fitted D* vertex to the original D0 decay vertex onto GEN D0 direction. The diagnostic is correlated with vertex error and can change sign due to weak localization; it is not independently a fit-code error or a new cut.

A simple residual–mass correlation is insufficient because the sign of momentum transport also depends on charge and relative azimuth. The existing geometric mechanism is preserved: an uncertain fitted endpoint changes the transported slow-pion phi and Δm. No causal claim is made from these pilot correlations alone.

#### 재현 자료

- pilot_targets.json: frozen selected candidate identities, original LFNs/GEN indices and weights.
- references_0..3.jsonl: freshly read official background points, multiplicities, GEN p4/event checks.
- pilot_states.json, pilot_stage_metrics.csv: selected existing fit-state and mass-stage data, sufficient for redoing metrics without remote reads.
- analyze.py, run_analysis.sh: analysis and plot reproduction.
- frameValidation.C / frameValidation_fullgen.C, frame_benchmark.csv / frame_benchmark_fullgen.csv: benchmark mapping tests. The v0 macro compilation failure and subsequent alternatives remain in logs; only completed v6/v7 data are used.
- extractReference.C, inputs_0..3.txt, run_reference.sh, reference_*.log: original event reads; 4 local worker processes, no batch submission.
- provenance.json, validation.json, checksums.json: source lineage, scope and completeness.

![Vertex residual pilot](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/vertex_truth_validation_20260929/vertex_residuals.png)

#### 남은 판정

This is a completed pilot, not final full-sample coverage calibration. To settle the original question across the analysis population, apply the frozen reference screen to all existing geometry candidates, keep every reference/matching failure explicit, and obtain original-event retained simulation truth for the anomalous candidates where possible. Do not apply an unconditional PV constraint to nonprompt Dstar. Fit bias, input covariance defects, wrong association and reference-transfer failure remain distinguishable hypotheses.
<!-- source:SRC-57:end -->

<a id="evd-01"></a>

## EVD-01 — production_readonly_audit_20260929/fix01_tracker_ep_pm/tracker_ep_pm.diff

**기록 구분:** 실제 수정 diff·정적/설정 검증·코드 이력. **원문:** [production_readonly_audit_20260929/fix01_tracker_ep_pm/tracker_ep_pm.diff](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix01_tracker_ep_pm/tracker_ep_pm.diff). **SHA256:** `3c27d1810ac132dff7cad82a28560ec0b3593d7e23faed48057f24db3b2e3fe5`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:EVD-01:start -->
```diff
--- a/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc
+++ b/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc
@@ -523,26 +523,26 @@
   // Tracker event plane angles (flat)
   eptrackmidAngle[0] = (hasEP ? (*eventplanes)[3].angle(2) : kInvalid);
   eptrackmidAngle[1] = (hasEP ? (*eventplanes)[9].angle(2) : kInvalid);
-  eptrackpAngle[0] = (hasEP ? (*eventplanes)[4].angle(2) : kInvalid);
-  eptrackpAngle[1] = (hasEP ? (*eventplanes)[10].angle(2) : kInvalid);
-  eptrackmAngle[0] = (hasEP ? (*eventplanes)[5].angle(2) : kInvalid);
-  eptrackmAngle[1] = (hasEP ? (*eventplanes)[11].angle(2) : kInvalid);
+  eptrackpAngle[0] = (hasEP ? (*eventplanes)[5].angle(2) : kInvalid);
+  eptrackpAngle[1] = (hasEP ? (*eventplanes)[11].angle(2) : kInvalid);
+  eptrackmAngle[0] = (hasEP ? (*eventplanes)[4].angle(2) : kInvalid);
+  eptrackmAngle[1] = (hasEP ? (*eventplanes)[10].angle(2) : kInvalid);
 
   // Tracker event plane angles (off)
   eptrackmidAngleoff[0] = (hasEP ? (*eventplanes)[3].angle(1) : kInvalid);
   eptrackmidAngleoff[1] = (hasEP ? (*eventplanes)[9].angle(1) : kInvalid);
-  eptrackpAngleoff[0] = (hasEP ? (*eventplanes)[4].angle(1) : kInvalid);
-  eptrackpAngleoff[1] = (hasEP ? (*eventplanes)[10].angle(1) : kInvalid);
-  eptrackmAngleoff[0] = (hasEP ? (*eventplanes)[5].angle(1) : kInvalid);
-  eptrackmAngleoff[1] = (hasEP ? (*eventplanes)[11].angle(1) : kInvalid);
+  eptrackpAngleoff[0] = (hasEP ? (*eventplanes)[5].angle(1) : kInvalid);
+  eptrackpAngleoff[1] = (hasEP ? (*eventplanes)[11].angle(1) : kInvalid);
+  eptrackmAngleoff[0] = (hasEP ? (*eventplanes)[4].angle(1) : kInvalid);
+  eptrackmAngleoff[1] = (hasEP ? (*eventplanes)[10].angle(1) : kInvalid);
 
   // Tracker event plane angles (raw)
   eptrackmidAngleRaw[0] = (hasEP ? (*eventplanes)[3].angle(0) : kInvalid);
   eptrackmidAngleRaw[1] = (hasEP ? (*eventplanes)[9].angle(0) : kInvalid);
-  eptrackpAngleRaw[0] = (hasEP ? (*eventplanes)[4].angle(0) : kInvalid);
-  eptrackpAngleRaw[1] = (hasEP ? (*eventplanes)[10].angle(0) : kInvalid);
-  eptrackmAngleRaw[0] = (hasEP ? (*eventplanes)[5].angle(0) : kInvalid);
-  eptrackmAngleRaw[1] = (hasEP ? (*eventplanes)[11].angle(0) : kInvalid);
+  eptrackpAngleRaw[0] = (hasEP ? (*eventplanes)[5].angle(0) : kInvalid);
+  eptrackpAngleRaw[1] = (hasEP ? (*eventplanes)[11].angle(0) : kInvalid);
+  eptrackmAngleRaw[0] = (hasEP ? (*eventplanes)[4].angle(0) : kInvalid);
+  eptrackmAngleRaw[1] = (hasEP ? (*eventplanes)[10].angle(0) : kInvalid);
 
   // Event plane angles (off)
   ephfAngleoff[0] = (hasEP ? (*eventplanes)[2].angle(1) : kInvalid);
@@ -569,10 +569,10 @@
   ephfQ[1] = (hasEP ? (*eventplanes)[8].q(2) : kInvalid);
   eptrackmidQ[0] = (hasEP ? (*eventplanes)[3].q(2) : kInvalid);
   eptrackmidQ[1] = (hasEP ? (*eventplanes)[9].q(2) : kInvalid);
-  eptrackpQ[0] = (hasEP ? (*eventplanes)[4].q(2) : kInvalid);
-  eptrackpQ[1] = (hasEP ? (*eventplanes)[10].q(2) : kInvalid);
-  eptrackmQ[0] = (hasEP ? (*eventplanes)[5].q(2) : kInvalid);
-  eptrackmQ[1] = (hasEP ? (*eventplanes)[11].q(2) : kInvalid);
+  eptrackpQ[0] = (hasEP ? (*eventplanes)[5].q(2) : kInvalid);
+  eptrackpQ[1] = (hasEP ? (*eventplanes)[11].q(2) : kInvalid);
+  eptrackmQ[0] = (hasEP ? (*eventplanes)[4].q(2) : kInvalid);
+  eptrackmQ[1] = (hasEP ? (*eventplanes)[10].q(2) : kInvalid);
 
   // sumw
   ephfmSumW = (hasEP ? (*eventplanes)[6].sumw() : kInvalid);
@@ -587,10 +587,10 @@
   eptrackmidSumW = (hasEP ? (*eventplanes)[3].sumw() : kInvalid);
   eptrackmidSumWSub[0] = (hasEP ? (*eventplanes)[3].sumw() : kInvalid);
   eptrackmidSumWSub[1] = (hasEP ? (*eventplanes)[9].sumw() : kInvalid);
-  eptrackpSumW[0] = (hasEP ? (*eventplanes)[4].sumw() : kInvalid);
-  eptrackpSumW[1] = (hasEP ? (*eventplanes)[10].sumw() : kInvalid);
-  eptrackmSumW[0] = (hasEP ? (*eventplanes)[5].sumw() : kInvalid);
-  eptrackmSumW[1] = (hasEP ? (*eventplanes)[11].sumw() : kInvalid);
+  eptrackpSumW[0] = (hasEP ? (*eventplanes)[5].sumw() : kInvalid);
+  eptrackpSumW[1] = (hasEP ? (*eventplanes)[11].sumw() : kInvalid);
+  eptrackmSumW[0] = (hasEP ? (*eventplanes)[4].sumw() : kInvalid);
+  eptrackmSumW[1] = (hasEP ? (*eventplanes)[10].sumw() : kInvalid);
 
   // sumCos/sumSin/sumPtOrEt
   ephfmsumCosRaw[0] = (hasEP ? (*eventplanes)[0].sumCos(0) : kInvalid);
@@ -637,27 +637,27 @@
   eptrackmidSumPtOrEt[0] = (hasEP ? (*eventplanes)[3].sumPtOrEt() : kInvalid);
   eptrackmidSumPtOrEt[1] = (hasEP ? (*eventplanes)[9].sumPtOrEt() : kInvalid);
 
-  eptrackpSumCosRaw[0] = (hasEP ? (*eventplanes)[4].sumCos(0) : kInvalid);
-  eptrackpSumCosRaw[1] = (hasEP ? (*eventplanes)[10].sumCos(0) : kInvalid);
-  eptrackpSumSinRaw[0] = (hasEP ? (*eventplanes)[4].sumSin(0) : kInvalid);
-  eptrackpSumSinRaw[1] = (hasEP ? (*eventplanes)[10].sumSin(0) : kInvalid);
-  eptrackpSumCos[0] = (hasEP ? (*eventplanes)[4].sumCos(2) : kInvalid);
-  eptrackpSumCos[1] = (hasEP ? (*eventplanes)[10].sumCos(2) : kInvalid);
-  eptrackpSumSin[0] = (hasEP ? (*eventplanes)[4].sumSin(2) : kInvalid);
-  eptrackpSumSin[1] = (hasEP ? (*eventplanes)[10].sumSin(2) : kInvalid);
-  eptrackpSumPtOrEt[0] = (hasEP ? (*eventplanes)[4].sumPtOrEt() : kInvalid);
-  eptrackpSumPtOrEt[1] = (hasEP ? (*eventplanes)[10].sumPtOrEt() : kInvalid);
-
-  eptrackmSumCosRaw[0] = (hasEP ? (*eventplanes)[5].sumCos(0) : kInvalid);
-  eptrackmSumCosRaw[1] = (hasEP ? (*eventplanes)[11].sumCos(0) : kInvalid);
-  eptrackmSumSinRaw[0] = (hasEP ? (*eventplanes)[5].sumSin(0) : kInvalid);
-  eptrackmSumSinRaw[1] = (hasEP ? (*eventplanes)[11].sumSin(0) : kInvalid);
-  eptrackmSumCos[0] = (hasEP ? (*eventplanes)[5].sumCos(2) : kInvalid);
-  eptrackmSumCos[1] = (hasEP ? (*eventplanes)[11].sumCos(2) : kInvalid);
-  eptrackmSumSin[0] = (hasEP ? (*eventplanes)[5].sumSin(2) : kInvalid);
-  eptrackmSumSin[1] = (hasEP ? (*eventplanes)[11].sumSin(2) : kInvalid);
-  eptrackmSumPtOrEt[0] = (hasEP ? (*eventplanes)[5].sumPtOrEt() : kInvalid);
-  eptrackmSumPtOrEt[1] = (hasEP ? (*eventplanes)[11].sumPtOrEt() : kInvalid);
+  eptrackpSumCosRaw[0] = (hasEP ? (*eventplanes)[5].sumCos(0) : kInvalid);
+  eptrackpSumCosRaw[1] = (hasEP ? (*eventplanes)[11].sumCos(0) : kInvalid);
+  eptrackpSumSinRaw[0] = (hasEP ? (*eventplanes)[5].sumSin(0) : kInvalid);
+  eptrackpSumSinRaw[1] = (hasEP ? (*eventplanes)[11].sumSin(0) : kInvalid);
+  eptrackpSumCos[0] = (hasEP ? (*eventplanes)[5].sumCos(2) : kInvalid);
+  eptrackpSumCos[1] = (hasEP ? (*eventplanes)[11].sumCos(2) : kInvalid);
+  eptrackpSumSin[0] = (hasEP ? (*eventplanes)[5].sumSin(2) : kInvalid);
+  eptrackpSumSin[1] = (hasEP ? (*eventplanes)[11].sumSin(2) : kInvalid);
+  eptrackpSumPtOrEt[0] = (hasEP ? (*eventplanes)[5].sumPtOrEt() : kInvalid);
+  eptrackpSumPtOrEt[1] = (hasEP ? (*eventplanes)[11].sumPtOrEt() : kInvalid);
+
+  eptrackmSumCosRaw[0] = (hasEP ? (*eventplanes)[4].sumCos(0) : kInvalid);
+  eptrackmSumCosRaw[1] = (hasEP ? (*eventplanes)[10].sumCos(0) : kInvalid);
+  eptrackmSumSinRaw[0] = (hasEP ? (*eventplanes)[4].sumSin(0) : kInvalid);
+  eptrackmSumSinRaw[1] = (hasEP ? (*eventplanes)[10].sumSin(0) : kInvalid);
+  eptrackmSumCos[0] = (hasEP ? (*eventplanes)[4].sumCos(2) : kInvalid);
+  eptrackmSumCos[1] = (hasEP ? (*eventplanes)[10].sumCos(2) : kInvalid);
+  eptrackmSumSin[0] = (hasEP ? (*eventplanes)[4].sumSin(2) : kInvalid);
+  eptrackmSumSin[1] = (hasEP ? (*eventplanes)[10].sumSin(2) : kInvalid);
+  eptrackmSumPtOrEt[0] = (hasEP ? (*eventplanes)[4].sumPtOrEt() : kInvalid);
+  eptrackmSumPtOrEt[1] = (hasEP ? (*eventplanes)[10].sumPtOrEt() : kInvalid);
 }
 
 void PATCompositeTreeProducer6::processRunInfo(const edm::Event& iEvent) {
```
<!-- source:EVD-01:end -->

<a id="evd-02"></a>

## EVD-02 — production_readonly_audit_20260929/fix01_tracker_ep_pm/verification.json

**기록 구분:** 실제 수정 diff·정적/설정 검증·코드 이력. **원문:** [production_readonly_audit_20260929/fix01_tracker_ep_pm/verification.json](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix01_tracker_ep_pm/verification.json). **SHA256:** `1865168954dff660c666ff4698fd9135389b3e65b2892b55a8cbfe8ccd1371f7`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:EVD-02:start -->
```json
{
  "status": "source-only static verification passed",
  "catalog": "/cvmfs/cms.cern.ch/el8_amd64_gcc11/cms/cmssw/CMSSW_13_2_11/src/RecoHI/HiEvtPlaneAlgos/interface/HiEvtPlaneList.h",
  "checked_assignments": 40,
  "mapping": [
    [
      "eptrackpAngle",
      0,
      5,
      "trackp2"
    ],
    [
      "eptrackpAngle",
      1,
      11,
      "trackp3"
    ],
    [
      "eptrackmAngle",
      0,
      4,
      "trackm2"
    ],
    [
      "eptrackmAngle",
      1,
      10,
      "trackm3"
    ],
    [
      "eptrackpAngleoff",
      0,
      5,
      "trackp2"
    ],
    [
      "eptrackpAngleoff",
      1,
      11,
      "trackp3"
    ],
    [
      "eptrackmAngleoff",
      0,
      4,
      "trackm2"
    ],
    [
      "eptrackmAngleoff",
      1,
      10,
      "trackm3"
    ],
    [
      "eptrackpAngleRaw",
      0,
      5,
      "trackp2"
    ],
    [
      "eptrackpAngleRaw",
      1,
      11,
      "trackp3"
    ],
    [
      "eptrackmAngleRaw",
      0,
      4,
      "trackm2"
    ],
    [
      "eptrackmAngleRaw",
      1,
      10,
      "trackm3"
    ],
    [
      "eptrackpQ",
      0,
      5,
      "trackp2"
    ],
    [
      "eptrackpQ",
      1,
      11,
      "trackp3"
    ],
    [
      "eptrackmQ",
      0,
      4,
      "trackm2"
    ],
    [
      "eptrackmQ",
      1,
      10,
      "trackm3"
    ],
    [
      "eptrackpSumW",
      0,
      5,
      "trackp2"
    ],
    [
      "eptrackpSumW",
      1,
      11,
      "trackp3"
    ],
    [
      "eptrackmSumW",
      0,
      4,
      "trackm2"
    ],
    [
      "eptrackmSumW",
      1,
      10,
      "trackm3"
    ],
    [
      "eptrackpSumCosRaw",
      0,
      5,
      "trackp2"
    ],
    [
      "eptrackpSumCosRaw",
      1,
      11,
      "trackp3"
    ],
    [
      "eptrackpSumSinRaw",
      0,
      5,
      "trackp2"
    ],
    [
      "eptrackpSumSinRaw",
      1,
      11,
      "trackp3"
    ],
    [
      "eptrackpSumCos",
      0,
      5,
      "trackp2"
    ],
    [
      "eptrackpSumCos",
      1,
      11,
      "trackp3"
    ],
    [
      "eptrackpSumSin",
      0,
      5,
      "trackp2"
    ],
    [
      "eptrackpSumSin",
      1,
      11,
      "trackp3"
    ],
    [
      "eptrackpSumPtOrEt",
      0,
      5,
      "trackp2"
    ],
    [
      "eptrackpSumPtOrEt",
      1,
      11,
      "trackp3"
    ],
    [
      "eptrackmSumCosRaw",
      0,
      4,
      "trackm2"
    ],
    [
      "eptrackmSumCosRaw",
      1,
      10,
      "trackm3"
    ],
    [
      "eptrackmSumSinRaw",
      0,
      4,
      "trackm2"
    ],
    [
      "eptrackmSumSinRaw",
      1,
      10,
      "trackm3"
    ],
    [
      "eptrackmSumCos",
      0,
      4,
      "trackm2"
    ],
    [
      "eptrackmSumCos",
      1,
      10,
      "trackm3"
    ],
    [
      "eptrackmSumSin",
      0,
      4,
      "trackm2"
    ],
    [
      "eptrackmSumSin",
      1,
      10,
      "trackm3"
    ],
    [
      "eptrackmSumPtOrEt",
      0,
      4,
      "trackm2"
    ],
    [
      "eptrackmSumPtOrEt",
      1,
      10,
      "trackm3"
    ]
  ],
  "only_authorized_40_index_changes": true,
  "patch_reverse_check_passed": true,
  "source_files_changed_since_audit": [
    "/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeAnalyzer/plugins/PATCompositeTreeProducer6.cc"
  ],
  "before_sha256": "369df37fb338d40dbe23214fb0d32eb2b197a6365814772d2f4b7411c36ee1dd",
  "after_sha256": "6f0988ec073e82caec243824246a6d188593f3fcac131ec7ee49a8ed99089f66",
  "build_run": false,
  "cmsRun_run": false,
  "existing_ROOT_modified": false
}
```
<!-- source:EVD-02:end -->

<a id="evd-03"></a>

## EVD-03 — production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/production_offline_selection.diff

**기록 구분:** 실제 수정 diff·정적/설정 검증·코드 이력. **원문:** [production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/production_offline_selection.diff](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/production_offline_selection.diff). **SHA256:** `67ca55ef8ad9f931cde5fe90cf0e017b140a3b32e7fa92f769c8db58d4c2cab2`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:EVD-03:start -->
```diff
--- a/VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py
+++ b/VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py
@@ -150,9 +150,16 @@
 
 # Add PbPb collision event selection
 process.load('VertexCompositeAnalysis.VertexCompositeProducer.collisionEventSelection_cff')
-process.load('VertexCompositeAnalysis.VertexCompositeProducer.hfCoincFilter_cff')
 process.load('VertexCompositeAnalysis.VertexCompositeProducer.hffilter_cfi')
-process.colEvtSel = cms.Sequence()
+# 2023 PbPb MiniAOD selection: CMS SWGuideHeavyIonCentrality / CmsHI 13_2_X.
+# Slimmed vertices do not retain track refs; use the official MiniAOD PV cut.
+process.primaryVertexFilter.src = cms.InputTag("offlineSlimmedPrimaryVertices")
+process.primaryVertexFilter.cut = cms.string("!isFake && abs(z) <= 25 && position.Rho <= 2")
+process.colEvtSel = cms.Sequence(
+    process.primaryVertexFilter
+    * process.clusterCompatibilityFilter
+    * process.phfCoincFilter2Th4
+)
 
 # Define the event selection sequence
 process.eventFilter_HM = cms.Sequence(
@@ -343,6 +350,7 @@
 
 process.dStarAna_step = cms.Path(
     process.eventFilter_HM
+    * process.colEvtSel
     * process.generalD0CandidatesNew
     * process.generalDStarCandidatesNew
     # * process.d0candCountFilter
@@ -358,7 +366,7 @@
 # eventinfoana must be in EndPath, and process.eventinfoana.selectEvents must be the name of a Path
 process.eventinfoana.selectEvents = cms.untracked.string('dStarAna_step')
 process.eventinfoana.triggerPathNames = cms.untracked.vstring(
-    "HLT_HIMinimumBiasHF1AND_v*", #24
+    "HLT_HIMinimumBiasHF1AND_v", #24
     "HLT_HIMinimumBiasHF1ANDZDC2nOR_v", #25
     "HLT_HIMinimumBiasHF1ANDZDC1nOR_v", #26
     )
@@ -366,6 +374,7 @@
     'Flag_colEvtSel',
     'Flag_hfCoincFilter',
     'Flag_primaryVertexFilter',
+    'Flag_clusterCompatibilityFilter',
     )
 process.eventinfoana.triggerFilterNames = cms.untracked.vstring()
 process.eventinfoana.stageL1Trigger = cms.uint32(2)
@@ -386,13 +395,18 @@
    process.pevt,
 )
 
-# Add the event selection filters
+# Individual flags use the HLT/unpacker prefix, not the combined offline selection.
+# evtSel order: combined, HF, PV, cluster compatibility.
 process.Flag_colEvtSel = cms.Path(process.eventFilter_HM * process.colEvtSel)
-#process.Flag_hfCoincFilter = cms.Path(process.eventFilter_HM * process.hfCoincFilter2Th4)
-process.Flag_primaryVertexFilter = cms.Path(process.eventFilter_HM * process.primaryVertexFilter * process.clusterCompatibilityFilter)
-# follow the exactly same config of process.eventinfoana.eventFilterNames
-#eventFilterPaths = [ process.Flag_colEvtSel , process.Flag_hfCoincFilter , process.Flag_primaryVertexFilter ]
-eventFilterPaths = [ process.Flag_colEvtSel  , process.Flag_primaryVertexFilter ]
+process.Flag_hfCoincFilter = cms.Path(process.eventFilter_HM * process.phfCoincFilter2Th4)
+process.Flag_primaryVertexFilter = cms.Path(process.eventFilter_HM * process.primaryVertexFilter)
+process.Flag_clusterCompatibilityFilter = cms.Path(process.eventFilter_HM * process.clusterCompatibilityFilter)
+eventFilterPaths = [
+    process.Flag_colEvtSel,
+    process.Flag_hfCoincFilter,
+    process.Flag_primaryVertexFilter,
+    process.Flag_clusterCompatibilityFilter,
+]
 for P in eventFilterPaths:
     process.schedule.insert(0, P)
 
--- a/VertexCompositeProducer/test/production/pbpb2023/data/PbPb2023_D0BothAndDStar_MB_cfg_v2_Step2MVA.py
+++ b/VertexCompositeProducer/test/production/pbpb2023/data/PbPb2023_D0BothAndDStar_MB_cfg_v2_Step2MVA.py
@@ -109,9 +109,16 @@
 
 # Add PbPb collision event selection
 process.load('VertexCompositeAnalysis.VertexCompositeProducer.collisionEventSelection_cff')
-process.load('VertexCompositeAnalysis.VertexCompositeProducer.hfCoincFilter_cff')
 process.load('VertexCompositeAnalysis.VertexCompositeProducer.hffilter_cfi')
-process.colEvtSel = cms.Sequence()
+# 2023 PbPb MiniAOD selection: CMS SWGuideHeavyIonCentrality / CmsHI 13_2_X.
+# Slimmed vertices do not retain track refs; use the official MiniAOD PV cut.
+process.primaryVertexFilter.src = cms.InputTag("offlineSlimmedPrimaryVertices")
+process.primaryVertexFilter.cut = cms.string("!isFake && abs(z) <= 25 && position.Rho <= 2")
+process.colEvtSel = cms.Sequence(
+    process.primaryVertexFilter
+    * process.clusterCompatibilityFilter
+    * process.phfCoincFilter2Th4
+)
 
 # Define the event selection sequence
 process.eventFilter_HM = cms.Sequence(
@@ -303,6 +310,7 @@
 
 process.dStarAna_step = cms.Path( 
     process.eventFilter_HM 
+    * process.colEvtSel
     * process.generalD0CandidatesNew
     * process.generalDStarCandidatesNew
     * process.d0candCountFilter
@@ -318,14 +326,15 @@
 # eventinfoana must be in EndPath, and process.eventinfoana.selectEvents must be the name of a Path
 process.eventinfoana.selectEvents = cms.untracked.string('dStarAna_step')
 process.eventinfoana.triggerPathNames = cms.untracked.vstring(
-    "HLT_HIMinimumBiasHF1AND_v*", #24
+    "HLT_HIMinimumBiasHF1AND_v", #24
     "HLT_HIMinimumBiasHF1ANDZDC2nOR_v", #25
     "HLT_HIMinimumBiasHF1ANDZDC1nOR_v", #26
     )
 process.eventinfoana.eventFilterNames = cms.untracked.vstring(
     'Flag_colEvtSel',
     'Flag_hfCoincFilter',
-    'Flag_primaryVertexFilter', 
+    'Flag_primaryVertexFilter',
+    'Flag_clusterCompatibilityFilter',
     )
 process.eventinfoana.triggerFilterNames = cms.untracked.vstring()
 process.eventinfoana.stageL1Trigger = cms.uint32(2)
@@ -346,13 +355,18 @@
    process.pevt,
 )
 
-# Add the event selection filters
+# Individual flags use the HLT/unpacker prefix, not the combined offline selection.
+# evtSel order: combined, HF, PV, cluster compatibility.
 process.Flag_colEvtSel = cms.Path(process.eventFilter_HM * process.colEvtSel)
-#process.Flag_hfCoincFilter = cms.Path(process.eventFilter_HM * process.hfCoincFilter2Th4)
-process.Flag_primaryVertexFilter = cms.Path(process.eventFilter_HM * process.primaryVertexFilter * process.clusterCompatibilityFilter)
-# follow the exactly same config of process.eventinfoana.eventFilterNames
-#eventFilterPaths = [ process.Flag_colEvtSel , process.Flag_hfCoincFilter , process.Flag_primaryVertexFilter ]
-eventFilterPaths = [ process.Flag_colEvtSel  , process.Flag_primaryVertexFilter ]
+process.Flag_hfCoincFilter = cms.Path(process.eventFilter_HM * process.phfCoincFilter2Th4)
+process.Flag_primaryVertexFilter = cms.Path(process.eventFilter_HM * process.primaryVertexFilter)
+process.Flag_clusterCompatibilityFilter = cms.Path(process.eventFilter_HM * process.clusterCompatibilityFilter)
+eventFilterPaths = [
+    process.Flag_colEvtSel,
+    process.Flag_hfCoincFilter,
+    process.Flag_primaryVertexFilter,
+    process.Flag_clusterCompatibilityFilter,
+]
 for P in eventFilterPaths:
     process.schedule.insert(0, P)
 
```
<!-- source:EVD-03:end -->

<a id="evd-04"></a>

## EVD-04 — production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/verification.json

**기록 구분:** 실제 수정 diff·정적/설정 검증·코드 이력. **원문:** [production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/verification.json](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/verification.json). **SHA256:** `c8ce3bdf1141f712449fc0274cfc362615d6eb280bf93a178febfcfaf06c4d55`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:EVD-04:start -->
```json
{
  "config_load_checks_passed": [
    "condor",
    "canonical"
  ],
  "framework": "CMSSW_13_2_11 el8_amd64_gcc11",
  "python_syntax_passed": true,
  "patch_applies_in_reverse": true,
  "new_lines_whitespace_clean": true,
  "changed_this_fix": [
    "VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py",
    "VertexCompositeProducer/test/production/pbpb2023/data/PbPb2023_D0BothAndDStar_MB_cfg_v2_Step2MVA.py",
    "PRODUCTION_AUDIT_FIXLIST_20260929.md"
  ],
  "after_sha256": {
    "VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py": "04d1d849a389f46de7407cb46def0f74eb38fe1d70dc13ed3e3ba6e1043b2341",
    "VertexCompositeProducer/test/production/pbpb2023/data/PbPb2023_D0BothAndDStar_MB_cfg_v2_Step2MVA.py": "5f4084dae1c5c20df2f60ca9de505172d9570f964d103d9e19787621880c4a15",
    "PRODUCTION_AUDIT_FIXLIST_20260929.md": "452163a17b20fa28952af2eb2bf6eb14253e1afad14c912f61917145a6955951"
  },
  "other_audited_source_changes_only_previously_authorized_PAT6_fix": true,
  "events_processed": 0,
  "built": false,
  "production_submitted": false
}
```
<!-- source:EVD-04:end -->

<a id="evd-05"></a>

## EVD-05 — production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/canonical_validation.json

**기록 구분:** 실제 수정 diff·정적/설정 검증·코드 이력. **원문:** [production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/canonical_validation.json](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/canonical_validation.json). **SHA256:** `7a0f8ed6ced600464ad09530432301be4deb234551813ded2c202545dbba8da1`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:EVD-05:start -->
```json
{
  "cfg": "/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/production/pbpb2023/data/PbPb2023_D0BothAndDStar_MB_cfg_v2_Step2MVA.py",
  "passed": true,
  "main_path": [
    "unpackedTracksAndVertices",
    "hltFilter",
    "primaryVertexFilter",
    "clusterCompatibilityFilter",
    "phfCoincFilter2Th4",
    "generalD0CandidatesNew",
    "generalDStarCandidatesNew",
    "d0candCountFilter",
    "hiEvtPlaneRecalc",
    "hiEvtPlaneFlatRecalc",
    "d0ana_newreduced",
    "dStarana",
    "eventplane"
  ],
  "scheduled_paths": [
    "Flag_clusterCompatibilityFilter",
    "Flag_primaryVertexFilter",
    "Flag_hfCoincFilter",
    "Flag_colEvtSel",
    "c",
    "eventFilter_HM_step",
    "dStarAna_step",
    "pevt"
  ],
  "flags": {
    "Flag_colEvtSel": [
      "unpackedTracksAndVertices",
      "hltFilter",
      "primaryVertexFilter",
      "clusterCompatibilityFilter",
      "phfCoincFilter2Th4"
    ],
    "Flag_hfCoincFilter": [
      "unpackedTracksAndVertices",
      "hltFilter",
      "phfCoincFilter2Th4"
    ],
    "Flag_primaryVertexFilter": [
      "unpackedTracksAndVertices",
      "hltFilter",
      "primaryVertexFilter"
    ],
    "Flag_clusterCompatibilityFilter": [
      "unpackedTracksAndVertices",
      "hltFilter",
      "clusterCompatibilityFilter"
    ]
  },
  "eventinfo_flag_order": [
    "Flag_colEvtSel",
    "Flag_hfCoincFilter",
    "Flag_primaryVertexFilter",
    "Flag_clusterCompatibilityFilter"
  ],
  "PV_cut": "!isFake && abs(z) <= 25 && position.Rho <= 2",
  "PV_src": "offlineSlimmedPrimaryVertices",
  "HF_input": "hiHFfilters:hiHFfilters",
  "HF_threshold_GeV": 4,
  "HF_min_each_side": 2,
  "runtime_events_processed": 0
}
```
<!-- source:EVD-05:end -->

<a id="evd-06"></a>

## EVD-06 — production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/condor_validation.json

**기록 구분:** 실제 수정 diff·정적/설정 검증·코드 이력. **원문:** [production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/condor_validation.json](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix02_pbpb2023_offline_selection/condor_validation.json). **SHA256:** `3e8d7816201767e50c1c81f195fc6d14da258bde53fbd7de281131a1158c3917`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:EVD-06:start -->
```json
{
  "cfg": "/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_Step2MVA_condor_v1.py",
  "passed": true,
  "main_path": [
    "unpackedTracksAndVertices",
    "hltFilter",
    "primaryVertexFilter",
    "clusterCompatibilityFilter",
    "phfCoincFilter2Th4",
    "generalD0CandidatesNew",
    "generalDStarCandidatesNew",
    "hiEvtPlaneRecalc",
    "hiEvtPlaneFlatRecalc",
    "d0ana_newreduced",
    "dStarana",
    "eventplane"
  ],
  "scheduled_paths": [
    "Flag_clusterCompatibilityFilter",
    "Flag_primaryVertexFilter",
    "Flag_hfCoincFilter",
    "Flag_colEvtSel",
    "c",
    "eventFilter_HM_step",
    "dStarAna_step",
    "pevt"
  ],
  "flags": {
    "Flag_colEvtSel": [
      "unpackedTracksAndVertices",
      "hltFilter",
      "primaryVertexFilter",
      "clusterCompatibilityFilter",
      "phfCoincFilter2Th4"
    ],
    "Flag_hfCoincFilter": [
      "unpackedTracksAndVertices",
      "hltFilter",
      "phfCoincFilter2Th4"
    ],
    "Flag_primaryVertexFilter": [
      "unpackedTracksAndVertices",
      "hltFilter",
      "primaryVertexFilter"
    ],
    "Flag_clusterCompatibilityFilter": [
      "unpackedTracksAndVertices",
      "hltFilter",
      "clusterCompatibilityFilter"
    ]
  },
  "eventinfo_flag_order": [
    "Flag_colEvtSel",
    "Flag_hfCoincFilter",
    "Flag_primaryVertexFilter",
    "Flag_clusterCompatibilityFilter"
  ],
  "PV_cut": "!isFake && abs(z) <= 25 && position.Rho <= 2",
  "PV_src": "offlineSlimmedPrimaryVertices",
  "HF_input": "hiHFfilters:hiHFfilters",
  "HF_threshold_GeV": 4,
  "HF_min_each_side": 2,
  "runtime_events_processed": 0
}
```
<!-- source:EVD-06:end -->

<a id="evd-07"></a>

## EVD-07 — production_readonly_audit_20260929/fix03_mc_offline_selection/mc_offline_selection.diff

**기록 구분:** 실제 수정 diff·정적/설정 검증·코드 이력. **원문:** [production_readonly_audit_20260929/fix03_mc_offline_selection/mc_offline_selection.diff](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix03_mc_offline_selection/mc_offline_selection.diff). **SHA256:** `f53d2f1331822d09685e2f9e0fa3c1afaddb1026f23810b14b1110fce9871510`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:EVD-07:start -->
```diff
--- a/VertexCompositeProducer/test/production/pbpb2023/mc/PbPb2023_D0BothAndDStar_MB_cfg_mc_v2_Step2MVA.py
+++ b/VertexCompositeProducer/test/production/pbpb2023/mc/PbPb2023_D0BothAndDStar_MB_cfg_mc_v2_Step2MVA.py
@@ -88,9 +88,16 @@
 
 # Add PbPb collision event selection
 process.load('VertexCompositeAnalysis.VertexCompositeProducer.collisionEventSelection_cff')
-process.load('VertexCompositeAnalysis.VertexCompositeProducer.hfCoincFilter_cff')
 process.load('VertexCompositeAnalysis.VertexCompositeProducer.hffilter_cfi')
-process.colEvtSel = cms.Sequence()
+# 2023 PbPb MiniAOD selection: CMS SWGuideHeavyIonCentrality / CmsHI 13_2_X.
+# Slimmed vertices do not retain track refs; use the official MiniAOD PV cut.
+process.primaryVertexFilter.src = cms.InputTag("offlineSlimmedPrimaryVertices")
+process.primaryVertexFilter.cut = cms.string("!isFake && abs(z) <= 25 && position.Rho <= 2")
+process.colEvtSel = cms.Sequence(
+    process.primaryVertexFilter
+    * process.clusterCompatibilityFilter
+    * process.phfCoincFilter2Th4
+)
 
 # Define the event selection sequence
 process.eventFilter_HM = cms.Sequence(
@@ -358,6 +365,7 @@
 
 process.dStarAna_step = cms.Path(
     process.eventFilter_HM
+    * process.colEvtSel
     * process.generalD0CandidatesNew
     * process.generalDStarCandidatesNew
     * process.d0candCountFilter
@@ -374,7 +382,7 @@
 # eventinfoana must be in EndPath, and process.eventinfoana.selectEvents must be the name of a Path
 process.eventinfoana.selectEvents = cms.untracked.string('dStarAna_step')
 process.eventinfoana.triggerPathNames = cms.untracked.vstring(
-    "HLT_HIMinimumBiasHF1AND_v*", #24
+    "HLT_HIMinimumBiasHF1AND_v", #24
     "HLT_HIMinimumBiasHF1ANDZDC2nOR_v", #25
     "HLT_HIMinimumBiasHF1ANDZDC1nOR_v", #26
     )
@@ -382,6 +390,7 @@
     'Flag_colEvtSel',
     'Flag_hfCoincFilter',
     'Flag_primaryVertexFilter',
+    'Flag_clusterCompatibilityFilter',
     )
 process.c = cms.Path(process.cent_seq)
 process.eventinfoana.triggerFilterNames = cms.untracked.vstring()
@@ -400,13 +409,18 @@
    process.pevt,
 )
 
-# Add the event selection filters
+# Individual flags use the HLT/unpacker prefix, not the combined offline selection.
+# evtSel order: combined, HF, PV, cluster compatibility.
 process.Flag_colEvtSel = cms.Path(process.eventFilter_HM * process.colEvtSel)
-#process.Flag_hfCoincFilter = cms.Path(process.eventFilter_HM * process.hfCoincFilter2Th4)
-process.Flag_primaryVertexFilter = cms.Path(process.eventFilter_HM * process.primaryVertexFilter * process.clusterCompatibilityFilter)
-# follow the exactly same config of process.eventinfoana.eventFilterNames
-#eventFilterPaths = [ process.Flag_colEvtSel , process.Flag_hfCoincFilter , process.Flag_primaryVertexFilter ]
-eventFilterPaths = [ process.Flag_colEvtSel  , process.Flag_primaryVertexFilter ]
+process.Flag_hfCoincFilter = cms.Path(process.eventFilter_HM * process.phfCoincFilter2Th4)
+process.Flag_primaryVertexFilter = cms.Path(process.eventFilter_HM * process.primaryVertexFilter)
+process.Flag_clusterCompatibilityFilter = cms.Path(process.eventFilter_HM * process.clusterCompatibilityFilter)
+eventFilterPaths = [
+    process.Flag_colEvtSel,
+    process.Flag_hfCoincFilter,
+    process.Flag_primaryVertexFilter,
+    process.Flag_clusterCompatibilityFilter,
+]
 for P in eventFilterPaths:
     process.schedule.insert(0, P)
 
--- a/VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_mc_Step2MVA_Condor_v1.py
+++ b/VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_mc_Step2MVA_Condor_v1.py
@@ -124,9 +124,16 @@
 
 # Add PbPb collision event selection
 process.load('VertexCompositeAnalysis.VertexCompositeProducer.collisionEventSelection_cff')
-process.load('VertexCompositeAnalysis.VertexCompositeProducer.hfCoincFilter_cff')
 process.load('VertexCompositeAnalysis.VertexCompositeProducer.hffilter_cfi')
-process.colEvtSel = cms.Sequence()
+# 2023 PbPb MiniAOD selection: CMS SWGuideHeavyIonCentrality / CmsHI 13_2_X.
+# Slimmed vertices do not retain track refs; use the official MiniAOD PV cut.
+process.primaryVertexFilter.src = cms.InputTag("offlineSlimmedPrimaryVertices")
+process.primaryVertexFilter.cut = cms.string("!isFake && abs(z) <= 25 && position.Rho <= 2")
+process.colEvtSel = cms.Sequence(
+    process.primaryVertexFilter
+    * process.clusterCompatibilityFilter
+    * process.phfCoincFilter2Th4
+)
 
 # Define the event selection sequence
 process.eventFilter_HM = cms.Sequence(
@@ -385,6 +392,7 @@
 #process.dStarAna_step = cms.Path( process.eventFilter_HM * process.generalD0CandidatesNew* process.generalDStarCandidatesNew * process.d0ana_newreduced *process.dStarana_mc*process.eventplane)
 process.dStarAna_step = cms.Path(
     process.eventFilter_HM
+    * process.colEvtSel
     * process.generalD0CandidatesNew
     * process.generalDStarCandidatesNew
     * process.d0candCountFilter
@@ -401,14 +409,15 @@
 # eventinfoana must be in EndPath, and process.eventinfoana.selectEvents must be the name of eventFilter_HM Path
 process.eventinfoana.selectEvents = cms.untracked.string('dStarAna_step')
 process.eventinfoana.triggerPathNames = cms.untracked.vstring(
-    "HLT_HIMinimumBiasHF1AND_v*", #24
+    "HLT_HIMinimumBiasHF1AND_v", #24
     "HLT_HIMinimumBiasHF1ANDZDC2nOR_v", #25
     "HLT_HIMinimumBiasHF1ANDZDC1nOR_v", #26
     )
 process.eventinfoana.eventFilterNames = cms.untracked.vstring(
     'Flag_colEvtSel',
     'Flag_hfCoincFilter',
-    'Flag_primaryVertexFilter', 
+    'Flag_primaryVertexFilter',
+    'Flag_clusterCompatibilityFilter',
     )
 process.centralityPath = cms.Path(process.cent_seq)
 process.eventinfoana.triggerFilterNames = cms.untracked.vstring()
@@ -427,13 +436,18 @@
    process.pevt,
 )
 
-# Add the event selection filters
+# Individual flags use the HLT/unpacker prefix, not the combined offline selection.
+# evtSel order: combined, HF, PV, cluster compatibility.
 process.Flag_colEvtSel = cms.Path(process.eventFilter_HM * process.colEvtSel)
-#process.Flag_hfCoincFilter = cms.Path(process.eventFilter_HM * process.hfCoincFilter2Th4)
-process.Flag_primaryVertexFilter = cms.Path(process.eventFilter_HM * process.primaryVertexFilter * process.clusterCompatibilityFilter)
-# follow the exactly same config of process.eventinfoana.eventFilterNames
-#eventFilterPaths = [ process.Flag_colEvtSel , process.Flag_hfCoincFilter , process.Flag_primaryVertexFilter ]
-eventFilterPaths = [ process.Flag_colEvtSel  , process.Flag_primaryVertexFilter ]
+process.Flag_hfCoincFilter = cms.Path(process.eventFilter_HM * process.phfCoincFilter2Th4)
+process.Flag_primaryVertexFilter = cms.Path(process.eventFilter_HM * process.primaryVertexFilter)
+process.Flag_clusterCompatibilityFilter = cms.Path(process.eventFilter_HM * process.clusterCompatibilityFilter)
+eventFilterPaths = [
+    process.Flag_colEvtSel,
+    process.Flag_hfCoincFilter,
+    process.Flag_primaryVertexFilter,
+    process.Flag_clusterCompatibilityFilter,
+]
 for P in eventFilterPaths:
     process.schedule.insert(0, P)
 
```
<!-- source:EVD-07:end -->

<a id="evd-08"></a>

## EVD-08 — production_readonly_audit_20260929/fix03_mc_offline_selection/canonical_validation.json

**기록 구분:** 실제 수정 diff·정적/설정 검증·코드 이력. **원문:** [production_readonly_audit_20260929/fix03_mc_offline_selection/canonical_validation.json](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix03_mc_offline_selection/canonical_validation.json). **SHA256:** `52ad36c6bf5238a98eaa4037634e228485962496eaa23b7e059e7239e3b167cb`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:EVD-08:start -->
```json
{
  "cfg": "/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/production/pbpb2023/mc/PbPb2023_D0BothAndDStar_MB_cfg_mc_v2_Step2MVA.py",
  "passed": true,
  "main_path": [
    "unpackedTracksAndVertices",
    "hltFilter",
    "primaryVertexFilter",
    "clusterCompatibilityFilter",
    "phfCoincFilter2Th4",
    "generalD0CandidatesNew",
    "generalDStarCandidatesNew",
    "d0candCountFilter",
    "hiEvtPlaneRecalc",
    "hiEvtPlaneFlatRecalc",
    "d0ana_newreduced",
    "dStarana_mc",
    "eventplane",
    "genDstarEventPlaneMiniAOD"
  ],
  "scheduled_paths": [
    "Flag_clusterCompatibilityFilter",
    "Flag_primaryVertexFilter",
    "Flag_hfCoincFilter",
    "Flag_colEvtSel",
    "c",
    "eventFilter_HM_step",
    "dStarAna_step",
    "pevt"
  ],
  "flags": {
    "Flag_colEvtSel": [
      "unpackedTracksAndVertices",
      "hltFilter",
      "primaryVertexFilter",
      "clusterCompatibilityFilter",
      "phfCoincFilter2Th4"
    ],
    "Flag_hfCoincFilter": [
      "unpackedTracksAndVertices",
      "hltFilter",
      "phfCoincFilter2Th4"
    ],
    "Flag_primaryVertexFilter": [
      "unpackedTracksAndVertices",
      "hltFilter",
      "primaryVertexFilter"
    ],
    "Flag_clusterCompatibilityFilter": [
      "unpackedTracksAndVertices",
      "hltFilter",
      "clusterCompatibilityFilter"
    ]
  },
  "eventinfo_flag_order": [
    "Flag_colEvtSel",
    "Flag_hfCoincFilter",
    "Flag_primaryVertexFilter",
    "Flag_clusterCompatibilityFilter"
  ],
  "PV_cut": "!isFake && abs(z) <= 25 && position.Rho <= 2",
  "PV_src": "offlineSlimmedPrimaryVertices",
  "HF_input": "hiHFfilters:hiHFfilters",
  "HF_threshold_GeV": 4,
  "HF_min_each_side": 2,
  "runtime_events_processed": 0
}
```
<!-- source:EVD-08:end -->

<a id="evd-09"></a>

## EVD-09 — production_readonly_audit_20260929/fix03_mc_offline_selection/condor_validation.json

**기록 구분:** 실제 수정 diff·정적/설정 검증·코드 이력. **원문:** [production_readonly_audit_20260929/fix03_mc_offline_selection/condor_validation.json](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/fix03_mc_offline_selection/condor_validation.json). **SHA256:** `6d16d73da01374509bed187ab3c6321388216fb9665f9ef7dc758ea388ce94c9`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:EVD-09:start -->
```json
{
  "cfg": "/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_mc_Step2MVA_Condor_v1.py",
  "passed": true,
  "main_path": [
    "unpackedTracksAndVertices",
    "hltFilter",
    "primaryVertexFilter",
    "clusterCompatibilityFilter",
    "phfCoincFilter2Th4",
    "generalD0CandidatesNew",
    "generalDStarCandidatesNew",
    "d0candCountFilter",
    "hiEvtPlaneRecalc",
    "hiEvtPlaneFlatRecalc",
    "d0ana_newreduced",
    "dStarana_mc",
    "eventplane",
    "genDstarEventPlaneMiniAOD"
  ],
  "scheduled_paths": [
    "Flag_clusterCompatibilityFilter",
    "Flag_primaryVertexFilter",
    "Flag_hfCoincFilter",
    "Flag_colEvtSel",
    "centralityPath",
    "eventFilter_HM_step",
    "dStarAna_step",
    "pevt"
  ],
  "flags": {
    "Flag_colEvtSel": [
      "unpackedTracksAndVertices",
      "hltFilter",
      "primaryVertexFilter",
      "clusterCompatibilityFilter",
      "phfCoincFilter2Th4"
    ],
    "Flag_hfCoincFilter": [
      "unpackedTracksAndVertices",
      "hltFilter",
      "phfCoincFilter2Th4"
    ],
    "Flag_primaryVertexFilter": [
      "unpackedTracksAndVertices",
      "hltFilter",
      "primaryVertexFilter"
    ],
    "Flag_clusterCompatibilityFilter": [
      "unpackedTracksAndVertices",
      "hltFilter",
      "clusterCompatibilityFilter"
    ]
  },
  "eventinfo_flag_order": [
    "Flag_colEvtSel",
    "Flag_hfCoincFilter",
    "Flag_primaryVertexFilter",
    "Flag_clusterCompatibilityFilter"
  ],
  "PV_cut": "!isFake && abs(z) <= 25 && position.Rho <= 2",
  "PV_src": "offlineSlimmedPrimaryVertices",
  "HF_input": "hiHFfilters:hiHFfilters",
  "HF_threshold_GeV": 4,
  "HF_min_each_side": 2,
  "runtime_events_processed": 0
}
```
<!-- source:EVD-09:end -->

<a id="evd-10"></a>

## EVD-10 — production_readonly_audit_20260929/document_reconciliation/verification.json

**기록 구분:** 실제 수정 diff·정적/설정 검증·코드 이력. **원문:** [production_readonly_audit_20260929/document_reconciliation/verification.json](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/document_reconciliation/verification.json). **SHA256:** `7dfeb1461a2a6bbcacb5e5813eb0d23123a94356cfa45fb1b949d6f373a81725`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:EVD-10:start -->
```json
{
  "VertexCompositeProducer/test/production/pbpb2023/mc/PbPb2023_D0BothAndDStar_MB_cfg_mc_v2_Step2MVA.py": {
    "current_sha256": "dfa322375a63eb5d603a0eaff7a25a4d8a9bf2af208b7d1174bb8c44f81e4066",
    "current_matches_recorded_fix03_diff": true
  },
  "VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_mc_Step2MVA_Condor_v1.py": {
    "current_sha256": "c654179b506b74e60cdbbcda45a61d4817b84fa293001e364d5f1225cf5c2e36",
    "current_matches_recorded_fix03_diff": true
  },
  "canonical_recorded_validation": {
    "cfg": "/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/production/pbpb2023/mc/PbPb2023_D0BothAndDStar_MB_cfg_mc_v2_Step2MVA.py",
    "passed": true,
    "main_path": [
      "unpackedTracksAndVertices",
      "hltFilter",
      "primaryVertexFilter",
      "clusterCompatibilityFilter",
      "phfCoincFilter2Th4",
      "generalD0CandidatesNew",
      "generalDStarCandidatesNew",
      "d0candCountFilter",
      "hiEvtPlaneRecalc",
      "hiEvtPlaneFlatRecalc",
      "d0ana_newreduced",
      "dStarana_mc",
      "eventplane",
      "genDstarEventPlaneMiniAOD"
    ],
    "scheduled_paths": [
      "Flag_clusterCompatibilityFilter",
      "Flag_primaryVertexFilter",
      "Flag_hfCoincFilter",
      "Flag_colEvtSel",
      "c",
      "eventFilter_HM_step",
      "dStarAna_step",
      "pevt"
    ],
    "flags": {
      "Flag_colEvtSel": [
        "unpackedTracksAndVertices",
        "hltFilter",
        "primaryVertexFilter",
        "clusterCompatibilityFilter",
        "phfCoincFilter2Th4"
      ],
      "Flag_hfCoincFilter": [
        "unpackedTracksAndVertices",
        "hltFilter",
        "phfCoincFilter2Th4"
      ],
      "Flag_primaryVertexFilter": [
        "unpackedTracksAndVertices",
        "hltFilter",
        "primaryVertexFilter"
      ],
      "Flag_clusterCompatibilityFilter": [
        "unpackedTracksAndVertices",
        "hltFilter",
        "clusterCompatibilityFilter"
      ]
    },
    "eventinfo_flag_order": [
      "Flag_colEvtSel",
      "Flag_hfCoincFilter",
      "Flag_primaryVertexFilter",
      "Flag_clusterCompatibilityFilter"
    ],
    "PV_cut": "!isFake && abs(z) <= 25 && position.Rho <= 2",
    "PV_src": "offlineSlimmedPrimaryVertices",
    "HF_input": "hiHFfilters:hiHFfilters",
    "HF_threshold_GeV": 4,
    "HF_min_each_side": 2,
    "runtime_events_processed": 0
  },
  "condor_recorded_validation": {
    "cfg": "/afs/cern.ch/user/j/junseok/analysis/dstarana/vertexcomposite/CMSSW_13_2_11/src/VertexCompositeAnalysis/VertexCompositeProducer/test/submission/condor/PbPb2023_D0BothAndDStar_MB_cfg_mc_Step2MVA_Condor_v1.py",
    "passed": true,
    "main_path": [
      "unpackedTracksAndVertices",
      "hltFilter",
      "primaryVertexFilter",
      "clusterCompatibilityFilter",
      "phfCoincFilter2Th4",
      "generalD0CandidatesNew",
      "generalDStarCandidatesNew",
      "d0candCountFilter",
      "hiEvtPlaneRecalc",
      "hiEvtPlaneFlatRecalc",
      "d0ana_newreduced",
      "dStarana_mc",
      "eventplane",
      "genDstarEventPlaneMiniAOD"
    ],
    "scheduled_paths": [
      "Flag_clusterCompatibilityFilter",
      "Flag_primaryVertexFilter",
      "Flag_hfCoincFilter",
      "Flag_colEvtSel",
      "centralityPath",
      "eventFilter_HM_step",
      "dStarAna_step",
      "pevt"
    ],
    "flags": {
      "Flag_colEvtSel": [
        "unpackedTracksAndVertices",
        "hltFilter",
        "primaryVertexFilter",
        "clusterCompatibilityFilter",
        "phfCoincFilter2Th4"
      ],
      "Flag_hfCoincFilter": [
        "unpackedTracksAndVertices",
        "hltFilter",
        "phfCoincFilter2Th4"
      ],
      "Flag_primaryVertexFilter": [
        "unpackedTracksAndVertices",
        "hltFilter",
        "primaryVertexFilter"
      ],
      "Flag_clusterCompatibilityFilter": [
        "unpackedTracksAndVertices",
        "hltFilter",
        "clusterCompatibilityFilter"
      ]
    },
    "eventinfo_flag_order": [
      "Flag_colEvtSel",
      "Flag_hfCoincFilter",
      "Flag_primaryVertexFilter",
      "Flag_clusterCompatibilityFilter"
    ],
    "PV_cut": "!isFake && abs(z) <= 25 && position.Rho <= 2",
    "PV_src": "offlineSlimmedPrimaryVertices",
    "HF_input": "hiHFfilters:hiHFfilters",
    "HF_threshold_GeV": 4,
    "HF_min_each_side": 2,
    "runtime_events_processed": 0
  },
  "FIT05": {
    "current_equals_git_HEAD": true,
    "introduced_commit": "74b53a3",
    "commit_date": "2026-07-27",
    "debug_guard_line": 470,
    "veto_line": 481,
    "source_sha256": "fd234f3357be61f6d2390244dc51425ab6532cff362497a10abe52d6391d1609"
  },
  "reviewed_at": "2026-09-29T12:23:40.683168+00:00"
}
```
<!-- source:EVD-10:end -->

<a id="evd-11"></a>

## EVD-11 — production_readonly_audit_20260929/document_reconciliation/FIT05_blame.txt

**기록 구분:** 실제 수정 diff·정적/설정 검증·코드 이력. **원문:** [production_readonly_audit_20260929/document_reconciliation/FIT05_blame.txt](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/production_readonly_audit_20260929/document_reconciliation/FIT05_blame.txt). **SHA256:** `7df9d0469df184b1eebad7dbf88e05ab8cb7878fc801aa571b0c70a5116c6cdc`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:EVD-11:start -->
```text
74b53a3d (JunseokLee3609 2026-07-27 08:29:22 +0200 469) 	      bool duplicateSlowPion = false;
74b53a3d (JunseokLee3609 2026-07-27 08:29:22 +0200 470) 	      if (debugCategoryCutflow_ && debugCat >= 0) {
74b53a3d (JunseokLee3609 2026-07-27 08:29:22 +0200 471) 	        reco::TrackRef d0Track0;
74b53a3d (JunseokLee3609 2026-07-27 08:29:22 +0200 472) 	        reco::TrackRef d0Track1;
74b53a3d (JunseokLee3609 2026-07-27 08:29:22 +0200 473) 	        if (const auto* rc0 = dynamic_cast<const reco::RecoChargedCandidate*>(dau0)) d0Track0 = rc0->track();
74b53a3d (JunseokLee3609 2026-07-27 08:29:22 +0200 474) 	        if (const auto* rc1 = dynamic_cast<const reco::RecoChargedCandidate*>(dau1)) d0Track1 = rc1->track();
74b53a3d (JunseokLee3609 2026-07-27 08:29:22 +0200 475) 	        if ((d0Track0.isNonnull() && d0Track0 == pionTrackRef) ||
74b53a3d (JunseokLee3609 2026-07-27 08:29:22 +0200 476) 	            (d0Track1.isNonnull() && d0Track1 == pionTrackRef)) {
74b53a3d (JunseokLee3609 2026-07-27 08:29:22 +0200 477) 	          duplicateSlowPion = true;
74b53a3d (JunseokLee3609 2026-07-27 08:29:22 +0200 478) 	          debugDuplicateTrack_[debugCat]++;
74b53a3d (JunseokLee3609 2026-07-27 08:29:22 +0200 479) 	        }
74b53a3d (JunseokLee3609 2026-07-27 08:29:22 +0200 480) 	      }
74b53a3d (JunseokLee3609 2026-07-27 08:29:22 +0200 481) 	      if (rejectDuplicateSlowPion_ && duplicateSlowPion) continue;
```
<!-- source:EVD-11:end -->

<a id="src-58"></a>

## SRC-58 — vertex_truth_validation_20260929/full_sample_20260929/REPORT.md

**기록 구분:** 조사 보고서·해석 (표본·시점별 결과). **원문:** [vertex_truth_validation_20260929/full_sample_20260929/REPORT.md](/eos/user/j/junseok/DStarRefitDiagnostics_20260918/vertex_truth_validation_20260929/full_sample_20260929/REPORT.md). **SHA256:** `1be5aa8ed4882e54daf55797b0b91dd4a37830817b041d67101c5da034f76702`.

이 절은 원본의 해당 시점 기록을 보존한다. 현재 완료/수정 상태는 위 결과 대장 및 후속 보고서를 함께 적용한다.

<!-- source:SRC-58:start -->
### D* vertex 위치와 오차: 기존 official 표본 확장 검사 — 2026-09-29

#### 결론과 범위

기존 64후보 pilot을, 보존된 common-vertex fit 상태와 MiniAOD에서 복원한 생성점이 모두 있는 **1,229후보**로 확장했다. Official pThat2/pT4 PR/NPR, reco pT 7–10 GeV/c, |y|<0.3, centrality 0–10%, tracker-axis folded |cosθ*| 0.6–0.8, DCA<0.08 cm, 원래 MVA>0의 고정 후보·weight를 사용했다. 새 fit이나 production 변경은 없다.

**확인된 저장 상태:** NPR·MVA≥0.95 후보 index 10257 (run:lumi:event 1:9188:149714471)은 vertex와 candidate state가 valid이지만 fitted vertex 3×3 covariance 고유값이 [-0.00679575, -0.0000101750, +0.00000431183] cm²다. 세 대각 성분도 모두 음수다. 원래 진단 ROOT의 diagDStarVertex와 별도 CMSSW replay의 fit_vertex 15개 값이 정확히 같다. 이 후보의 오차로 pull을 계산하거나 행렬을 보정하지 않았다. 한 후보를 peak 원인 또는 전체 발생률로 확대하지 않는다.

**조건부 관측:** 양의 정부호 vertex covariance를 가진 1,228후보에 큰 비행방향 잔차 tail이 있다. PR·MVA≥0.95에서 pull 표준편차 1.379 (event bootstrap 95% 구간 1.087–1.664), |pull|>3 후보 13/491개다. 그중 original slow-track covariance가 PSD인 165후보에도 큰 pull이 4개 있다. Indefinite 입력 covariance 하나만으로 모든 큰 잔차가 설명되지는 않는다. 그러나 본 결과만으로 중앙 Δm 돌출의 원인이나 전체 fit uncertainty coverage를 확정하지 않는다.

#### 입력·정의

- Geometry 기록: truth_geometry/geometry_records.json (1,264후보의 fitted vertex, covariance, GEN D0 방향).
- 생성점 기록: embedded_truth/records.json (1,229후보의 background translation과 signal slow-pion GEN production vertex). Geometry 1,264개에서 35개 NPR 기록이 없고, 원래 1,281개 조사 표본에서 geometry 전에 17개 NPR 후보가 빠졌다.
- 원래 후보·weight·MVA: vertex_truth_validation_20260929/official_groups.json과 truth_geometry/candidates.csv. MVA 통과는 원래 score≥0.95, 미통과는 0<MVA<0.95.
- 기준점 = background root 위치 + matched signal slow-pion GEN production vertex. NPR의 B 비행 위치를 PV로 강제하지 않는다. 기존 EmbeddedTruthProbe 수락 조건은 background root≥2, nucleon root≥2, root spread≤1e−6 cm이다. 원래 official event별 독립 SimVertex/smeared HepMC 직접 대조는 아니다.
- 잔차 r = fitted D* vertex − 위 기준점. 비행방향 n은 matched GEN D0 momentum의 단위벡터다. Pull = n·r / sqrt(nᵀ C_vertex n). 3D 검사는 전체 3×3 covariance를 쓴다.

CMS 공식 [Vertex Fitting의 Goal of the page](https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideVertexFitting)는 fit 출력의 “position, covariance matrix”와 성공 지표를 구분한다. [Kinematic Vertex Fit의 The KinematicParticle and KinematicVertex](https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideKinematicVertexFit)는 vertex 위치·covariance·χ²·ndf 저장을 설명한다. 이에 따라 valid 여부와 오차 품질을 별도로 확인했다.

#### 비행방향 결과

95% 구간은 고정된 표본과 weight에 조건부인 **2,000회 event 단위 Poisson bootstrap**이다. N_eff=(sumw)²/sumw2는 후보 수가 아니다. ±1σ/±2σ 열은 |pull|≤1/2의 가중 비율이다. 명목 정규분포의 68.3%/95.4%를 완전한 coverage 인증 기준으로 사용하지 않는다.

|그룹|N / N_eff|event|중앙 σ (mm)|pull SD [95% 구간]|±1σ [95% 구간]|±2σ [95% 구간]|큰 pull 수|
|---|---:|---:|---:|---:|---:|---:|---:|
|PR, MVA≥0.95|491 / 485.8|485|1.854|1.379 [1.087,1.664]|73.4% [69.6,77.4]|92.6% [90.2,94.8]|13|
|PR, 0<MVA<0.95|225 / 225.0|222|1.773|1.126 [0.981,1.259]|70.2% [64.3,76.0]|93.3% [89.9,96.4]|6|
|NPR, MVA≥0.95|350 / 344.6|345|1.843|1.216 [0.992,1.462]|74.9% [70.3,79.4]|93.9% [91.3,96.2]|10|
|NPR, 0<MVA<0.95|162 / 159.5|158|1.796|1.116 [0.969,1.250]|63.6% [56.2,70.9]|93.1% [88.9,96.9]|2|

큰 pull은 |pull|>3이다. NPR MVA≥0.95의 선택 후보는 351개지만 index 10257의 covariance가 indefinite여서 pull 대상은 350개다. 전체 행과 제외 이유는 candidates.csv, sumw·sumw2·3D Mahalanobis 비율은 group_metrics.csv에 있다. PR pass index 1506은 +11.07 pull (12.49 mm / 1.13 mm, χ²/ndf=0.714/1, original slow covariance PSD), index 3148은 −6.59 (−10.81 mm / 1.64 mm, χ²/ndf=1.539/1)이다. 작은 χ²도 위치 정확성을 보증하지 않는다. 이 큰-pull 대표 후보들의 Δm는 주로 중앙 bin 밖이므로 중앙 돌출의 직접 원인이라고 주장하지 않는다.

#### 재현 검증

1. 기존 64후보 pilot 중 이번 생성점 기록과 겹치는 62개의 세 축 잔차·σ·pull을 재현했다. 최대 절대 차이 4.53e−13이다. 나머지 index 9982, 10235는 누락 NPR 35개에 속한다.
2. 큰 pull 8개와 indefinite vertex covariance 1개, 총 9개 원본 MiniAOD event를 ROOT 해석 실행으로 다시 읽었다. 9/9 성공. 모두 background modal point가 유일하고 count≥10·fraction>0.5이며, 새 modal point는 기존 background-root point와 정확히 같다. 나머지 1,220개에 이 modal screen을 적용했다고 주장하지 않는다.
3. Index 10257은 background 74/74가 한 지점이고, 보존 진단의 vertex 15개 값과 CMSSW replay 값이 같다. valid=1, χ²=7.07366, ndf=1이면서 vertex covariance가 indefinite다.
4. 분석 코드에서 candidate/event/GEN state/weight/MVA 연결을 assert했고, 실제 후보 1,229행과 그룹 결과 4행을 목적지에서 확인했다.

재현 명령: python3 analyze.py; bash run_reference.sh; python3 validate_references.py. 두 번째 명령은 root -l -b -q extractReference.C(0)를 해석 실행한다. ROOT macro 컴파일은 하지 않았다. 입력 해시는 validation.json, fresh reference receipt는 reference_validation.json에 있다. Production 코드·cfg·기존 ROOT는 수정하지 않았다.

#### 해석 한계와 다음 판단

이 결과는 **source-derived 기준점에 조건부인 vertex 진단**이다. 원래 official event의 독립 SimVertex/smeared HepMC와 hit-level track association이 없어 모든 event의 reference·GEN matching을 보증하지 못한다. 9개 외 후보의 독립 modal screen은 아직 없고, NPR 52개가 앞선 재생·추출 단계에서 빠져 있다. Bootstrap은 event 재표본만 포함하며 reference 추정, weight 결정, 중복 GEN 후보, model·selection 및 full-chain covariance를 포함하지 않는다.

다음 검사는 누락 NPR 입력을 회복하고 전 후보에 독립 modal screen과 가능한 원래 SIM truth 대조를 적용하는 것이다. 이후 동일 후보의 vertex pull을 Δm 이동·GEN mass 정확도와 연결해야 한다. 이 결과만으로 clipping, 후보 제거, PV 제약, refit 교체 또는 data/MC 전체 재생산을 채택하지 않는다.
<!-- source:SRC-58:end -->

<!-- full-investigation-corpus:end -->
