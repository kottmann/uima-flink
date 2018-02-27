import java.io.Serializable;

import org.apache.uima.ResourceSpecifierFactory;
import org.apache.uima.UIMAFramework;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.TypeSystem;
import org.apache.uima.fit.internal.ResourceManagerFactory;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceManager;
import org.apache.uima.resource.metadata.FsIndexDescription;
import org.apache.uima.resource.metadata.TypePriorities;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.resource.metadata.impl.FsIndexDescription_impl;
import org.apache.uima.util.CasCreationUtils;

public class CASFactory implements Serializable {

  private final TypeSystemDescription typeSystemDescription;

  private ResourceSpecifierFactory resourceSpecifierFactory;
  private TypePriorities typePriorities;
  private FsIndexDescription indexDesciptor;
  private TypeSystem typeSystem;
  private ResourceManager resourceManager;

  public CASFactory(TypeSystemDescription typeSystemDescription) {
    this.typeSystemDescription = typeSystemDescription;


//    this.resourceManager = resourceManager;
  }

  private synchronized void init() throws ResourceInitializationException {
    if (typeSystem == null) {
      this.resourceSpecifierFactory = UIMAFramework.getResourceSpecifierFactory();

      this.typePriorities = resourceSpecifierFactory.createTypePriorities();

      this.indexDesciptor = new FsIndexDescription_impl();
      this.indexDesciptor.setLabel("TOPIndex");
      this.indexDesciptor.setTypeName("uima.cas.TOP");
      this.indexDesciptor.setKind(FsIndexDescription.KIND_SORTED);

      // hack, any way to avoid creating a new cas for this?
      this.typeSystem = createEmptyCAS(typeSystemDescription, resourceSpecifierFactory,
          typePriorities, indexDesciptor).getTypeSystem();

      this.resourceManager = ResourceManagerFactory.newResourceManager();
    }

  }

  public CAS createEmptyCAS() {
    CAS cas;
    try {
      if (typeSystem == null) {
        init();
      }
      cas = CasCreationUtils.createCas(typeSystem, typePriorities,
          new FsIndexDescription[] { indexDesciptor }, null, null);
    } catch (ResourceInitializationException e) {
      e.printStackTrace();
      cas = null;
    }

    return cas;
  }

  private static CAS createEmptyCAS(TypeSystemDescription typeSystem,
                                   ResourceSpecifierFactory resourceSpecifierFactory,
                                   TypePriorities typePriorities,
                                   FsIndexDescription indexDesciptor) {
    CAS cas;
    try {
      cas = CasCreationUtils.createCas(typeSystem, typePriorities,
          new FsIndexDescription[] { indexDesciptor });
    } catch (ResourceInitializationException e) {
      e.printStackTrace();
      cas = null;
    }

    return cas;
  }
}
