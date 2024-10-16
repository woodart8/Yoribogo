package com.avengers.yoribogo.recipeboard.controller;

import com.avengers.yoribogo.recipeboard.domain.RecipeBoardComment;
import com.avengers.yoribogo.recipeboard.dto.RecipeBoardCommentDTO;
import com.avengers.yoribogo.recipeboard.dto.RecipeBoardRecommentDTO;
import com.avengers.yoribogo.recipeboard.service.RecipeBoardCommentServiceImpl;
import com.avengers.yoribogo.recipeboard.service.RecipeBoardRecommentServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/recipe-board/{recipeBoardId}/comments")
public class RecipeBoardCommentController {
    private final RecipeBoardCommentServiceImpl recipeBoardCommentService;
    private final RecipeBoardRecommentServiceImpl recipeBoardRecommentService;


    @Autowired
    public RecipeBoardCommentController(RecipeBoardCommentServiceImpl recipeBoardCommentService,
                                        RecipeBoardRecommentServiceImpl recipeBoardRecommentService) {
        this.recipeBoardCommentService = recipeBoardCommentService;
        this.recipeBoardRecommentService = recipeBoardRecommentService;


    }




    // 댓글 등록
    @PostMapping
    public ResponseEntity<RecipeBoardCommentDTO> createComment(
            @PathVariable Long recipeBoardId,
            @RequestBody RecipeBoardCommentDTO commentDTO){

        commentDTO.setRecipeBoardId(recipeBoardId);

        RecipeBoardCommentDTO createdComment = recipeBoardCommentService.createComment(commentDTO);
        return new ResponseEntity<>(createdComment, HttpStatus.CREATED);

    }



    // 댓글 수정
    @PutMapping("/{id}")
    public ResponseEntity<RecipeBoardCommentDTO> modifyComment(
            @PathVariable Long id,
            @RequestBody RecipeBoardCommentDTO commentDTO){

        RecipeBoardCommentDTO updatedComment = recipeBoardCommentService.modifyComment(id, commentDTO);
        return new ResponseEntity<>(updatedComment, HttpStatus.OK);
    }



    // 댓글 삭제
    @DeleteMapping("/{id}")
    public ResponseEntity<RecipeBoardComment> deleteComment(@PathVariable Long id){

        recipeBoardCommentService.deleteComment(id);
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);

    }



    // 댓글 게시글별 조회
    @GetMapping
    public ResponseEntity<List<RecipeBoardCommentDTO>> getCommentsByRecipeBoardId(
            @PathVariable Long recipeBoardId){

        List<RecipeBoardCommentDTO> commentsByRecipeBoardId = recipeBoardCommentService.getCommentsByRecipeBoardId(recipeBoardId);
        return new ResponseEntity<>(commentsByRecipeBoardId, HttpStatus.OK);
    }

    // 대댓글 게시글별 조회
    @GetMapping("/recomments")
    public ResponseEntity<List<RecipeBoardRecommentDTO>> getRecommentsByRecipeBoardId(
            @PathVariable Long recipeBoardId) {

        List<RecipeBoardRecommentDTO> recomments = recipeBoardRecommentService.getRecommentsByRecipeId(recipeBoardId);
        return new ResponseEntity<>(recomments, HttpStatus.OK);
    }


    // 댓글 회원별 조회
    @GetMapping("/user/{userId}")
    public ResponseEntity<List<RecipeBoardCommentDTO>> getRecommentsByUserId(
            @PathVariable Long userId,
            @PathVariable Long recipeBoardId){

        List<RecipeBoardCommentDTO> commentsByUserId = recipeBoardCommentService.getCommentsByUserId(userId);
        return new ResponseEntity<>(commentsByUserId, HttpStatus.OK);
    }

    // 대댓글 회원별 조회
    @GetMapping("/recomments/user/{userId}")
    public ResponseEntity<List<RecipeBoardRecommentDTO>> getRecommentsByUserId(
            @PathVariable Long userId){

        List<RecipeBoardRecommentDTO> recommentsByUserId = recipeBoardRecommentService.getRecommentsByUserId(userId);
        return new ResponseEntity<>(recommentsByUserId, HttpStatus.OK);



    }


}
