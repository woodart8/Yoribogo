package com.avengers.yoribogo.recipe.service;

import com.avengers.yoribogo.common.exception.CommonException;
import com.avengers.yoribogo.common.exception.ErrorCode;
import com.avengers.yoribogo.openai.service.OpenAIService;
import com.avengers.yoribogo.recipe.domain.Recipe;
import com.avengers.yoribogo.recipe.domain.RecipeManual;
import com.avengers.yoribogo.recipe.dto.RecipeManualDTO;
import com.avengers.yoribogo.recipe.dto.RequestAIRecipeManualDTO;
import com.avengers.yoribogo.recipe.dto.RequestRecipeManualDTO;
import com.avengers.yoribogo.recipe.repository.RecipeManualRepository;
import com.avengers.yoribogo.recipe.repository.RecipeRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;

import java.util.*;

@Slf4j
@Service
public class RecipeManualServiceImpl implements RecipeManualService {

    private final ModelMapper modelMapper;
    private final RecipeManualRepository recipeManualRepository;
    private final RecipeRepository recipeRepository;
    private final OpenAIService openAIService;

    @Autowired
    public RecipeManualServiceImpl(ModelMapper modelMapper,
                                   RecipeRepository recipeRepository,
                                   RecipeManualRepository recipeManualRepository,
                                   OpenAIService openAIService) {
        this.modelMapper = modelMapper;
        this.recipeRepository = recipeRepository;
        this.recipeManualRepository = recipeManualRepository;
        this.openAIService = openAIService;
    }

    // 요리 레시피 아이디로 매뉴얼 조회
    @Override
    public List<RecipeManualDTO> findRecipeManualByRecipeId(Long recipeId) {
        // 기존 엔티티 목록 조회
        List<RecipeManual> recipeManualList = recipeManualRepository.findByRecipeId(recipeId);

        // 조회된게 없을 경우 예외처리
        if (recipeManualList.isEmpty()) {
            throw new CommonException(ErrorCode.NOT_FOUND_RECIPE_MANUAL);
        }

        // RecipeManual -> RecipeManualDTO 변환 및 List 반환
        return convertEntityToDTO(recipeManualList);
    }

    // 요리 레시피 매뉴얼 등록
    @Override
    @Transactional
    public List<RecipeManualDTO> registRecipeManual(Long recipeId, RequestRecipeManualDTO requestRecipeManualDTO) {
        Recipe recipe = recipeRepository.findById(recipeId)
                .orElseThrow(() -> new CommonException(ErrorCode.NOT_FOUND_RECIPE));

        List<RecipeManual> recipeManualList = new ArrayList<>();

        List<Map<String,String>> manual = requestRecipeManualDTO.getManual();
        for(int i = 0; i < manual.size(); i++) {
            // 매뉴얼 생성
            RecipeManualDTO newRecipeManualDTO = RecipeManualDTO
                    .builder()
                    .recipeManualStep(i+1)
                    .manualMenuImage(manual.get(i).get("image"))
                    .manualContent(manual.get(i).get("content"))
                    .build();

            // 매뉴얼 등록
            RecipeManual recipeManual = modelMapper.map(newRecipeManualDTO, RecipeManual.class);
            recipeManual.setRecipe(recipe);
            recipeManualList.add(recipeManualRepository.save(recipeManual));
        }

        // RecipeManual -> RecipeManualDTO 변환 및 List 반환
        return convertEntityToDTO(recipeManualList);
    }

    // 요리 레시피 매뉴얼 수정
    @Override
    @Transactional
    public List<RecipeManualDTO> modifyRecipeManual(Long recipeId, RequestRecipeManualDTO requestRecipeManualDTO) {
        // 기존 엔티티 목록 조회
        List<RecipeManual> recipeManualList =
                recipeManualRepository.findByRecipeId(recipeId);

        // 조회된게 없을 경우 예외처리
        if (recipeManualList.isEmpty()) {
            throw new CommonException(ErrorCode.NOT_FOUND_RECIPE_MANUAL);
        }

        // 기존 엔티티 삭제
        recipeManualRepository.deleteAll(recipeManualList);

        // 바뀐 매뉴얼 등록
        return registRecipeManual(recipeId, requestRecipeManualDTO);
    }

    // AI 생성 매뉴얼 등록 및 비동기로 결과 반환
    @Override
    public Flux<String> registAIRecipeManual(Long recipeId, RequestAIRecipeManualDTO requestAIRecipeManualDTO) {
        // 유효성 검사
        if (requestAIRecipeManualDTO.getMenuName() == null || requestAIRecipeManualDTO.getMenuIngredient() == null) {
            return Flux.error(new CommonException(ErrorCode.INVALID_REQUEST_BODY));  // 유효하지 않을 경우 Flux.error로 반환
        }

        // 프롬프트 생성
        String recipePrompt = requestAIRecipeManualDTO.getMenuName() + "에 필요한 재료가 " +
                requestAIRecipeManualDTO.getMenuIngredient() + "일 때, " +
                requestAIRecipeManualDTO.getMenuName() + "의 레시피를 최대 6단계로 요약해줘. " +
                "각 단계에 번호만 붙여 '1. 쌀을 씻습니다.'와 같은 형식으로 간결하게 작성해줘.";

        // OpenAI API 호출로부터 Flux<String>을 반환
        return Flux.defer(() -> {
            try {
                String recommendationHolder = "";

                // OpenAI API 호출
                return openAIService.getRecommendManuals(recipePrompt)
                        .doOnNext(recommendation -> {
                            // 추천 매뉴얼을 받아오는 중의 로깅
                            log.info("Received recommendation: {}", recommendation);
                            recommendationHolder += recommendation; // recommendation 저장
                        })
                        .doOnComplete(() -> {
                            // 완료되었을 때 registRecipeManual 호출
                            if (recommendationHolder != null) {
                                registRecipeManual(recipeId, recommendationHolder); // recommendation을 전달
                            } else {
                                log.warn("No recommendation received.");
                            }
                        })
                        .doOnError(error -> {
                            // 에러 발생 시 로깅
                            log.error("Error occurred while getting recommendations: {}", error.getMessage());
                        });
            } catch (JsonProcessingException e) {
                return Flux.error(new CommonException(ErrorCode.INTERNAL_SERVER_ERROR));  // 내부 서버 에러 처리
            }
        });
    }

    // AI 생성 매뉴얼 등록에 사용하는 메소드
    private void registRecipeManual(Long recipeId, String aiAnswerRecipe) {
        Recipe recipe = recipeRepository.findById(recipeId)
                .orElseThrow(() -> new CommonException(ErrorCode.NOT_FOUND_RECIPE));

        // AI가 생성한 요리 레시피 매뉴얼 등록
        List<Map<String, String>> manual = new ArrayList<>();

        List<String> contents = Arrays.stream(aiAnswerRecipe.split("\n")).toList();
        for (String content : contents) {
            Map<String, String> map = new HashMap<>();
            map.put("content", content);
            manual.add(map);
        }

        for(int i = 0; i < manual.size(); i++) {
            // 매뉴얼 생성
            RecipeManualDTO newRecipeManualDTO = RecipeManualDTO
                    .builder()
                    .recipeManualStep(i + 1)
                    .manualMenuImage(manual.get(i).get("image"))
                    .manualContent(manual.get(i).get("content"))
                    .build();

            // 매뉴얼 등록
            RecipeManual recipeManual = modelMapper.map(newRecipeManualDTO, RecipeManual.class);
            recipeManual.setRecipe(recipe);
            recipeManualRepository.save(recipeManual);
        }
    }

    // RecipeManual -> RecipeManualDTO 변환 및 List 반환 메소드
    private List<RecipeManualDTO> convertEntityToDTO(List<RecipeManual> recipeManualList) {
        return recipeManualList.stream()
                .map(recipeManual -> modelMapper.map(recipeManual, RecipeManualDTO.class))
                .toList();
    }

}
