package com.avengers.yoribogo.recipe.service;

import com.avengers.yoribogo.recipe.dto.GoodMenuDTO;
import com.avengers.yoribogo.recipe.dto.RecommendedMenuDTO;

import java.util.List;

public interface RecommendedMenuService {

    // 추천 요리 회원별 조회
    List<GoodMenuDTO> findRecommendedMenuByUserId(Long userId);

    // 추천 요리 등록
    RecommendedMenuDTO registRecommendedMenu(RecommendedMenuDTO registRecommendedMenuDTO);

    // 추천 요리 삭제
    void removeRecommendedMenu(Long recommendedMenuId);

}
