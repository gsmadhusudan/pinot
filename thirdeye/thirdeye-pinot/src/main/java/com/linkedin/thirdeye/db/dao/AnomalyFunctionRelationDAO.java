package com.linkedin.thirdeye.db.dao;

import com.google.common.collect.ImmutableMap;
import com.google.inject.persist.Transactional;
import java.util.List;

import com.linkedin.thirdeye.db.entity.AnomalyFunctionRelation;

public class AnomalyFunctionRelationDAO extends AbstractJpaDAO<AnomalyFunctionRelation> {
  public AnomalyFunctionRelationDAO() {
    super(AnomalyFunctionRelation.class);
  }

  private static final String DELETE_BY_PARENT_ID = "DELETE FROM AnomalyFunctionRelation r WHERE r.parentId = :parentId";
  private static final String DELETE_BY_PARENT_CHILD = "DELETE FROM AnomalyFunctionRelation r WHERE r.parentId = :parentId and r.childId = :childId";

  @Transactional
  public void deleteByParent(Long parentId) {
    getEntityManager().createQuery(DELETE_BY_PARENT_ID, entityClass)
        .setParameter("parentId", parentId).executeUpdate();
  }

  @Transactional
  public void deleteByParentChild(Long parentId, Long childId) {
    getEntityManager().createQuery(DELETE_BY_PARENT_CHILD, entityClass)
        .setParameter("parentId", parentId).setParameter("childId", childId).executeUpdate();
  }

  @Transactional
  public List<AnomalyFunctionRelation> findByParent(Long parentId) {
    return super.findByParams(ImmutableMap.of("parentId", parentId));
  }
}
