import React, { useContext, useEffect} from 'react';
import ReactDOM from 'react-dom';
import { AyxAppWrapper, Box, Grid, Typography, makeStyles, Theme, TextField} from '@alteryx/ui';
import { Alteryx} from '@alteryx/icons';
import { Context as UiSdkContext, DesignerApi } from '@alteryx/react-comms';




const App = () => {
  const [model, handleUpdateModel] = useContext(UiSdkContext);

  const onHandleTextChange = event => {
    const newModel = { ...model };
    newModel.Configuration[event.target.id] = event.target.value;
    handleUpdateModel(newModel);
  };
  

  return (
    <Box p={4}>
     <Grid container spacing={4} direction="column">
        <Grid item>
          <Typography variant="h3">
            QVD Input Tool
          </Typography>
        </Grid>
		<Grid item>
			<TextField
			  fullWidth
			  id="QVDFile"
			  label="QVD File"
			  placeholder="[QVD File Path...]"
			  onChange={onHandleTextChange}
			  value={model.Configuration.QVDFile}
			/>
		</Grid>
		
      </Grid>
    </Box>
  )
}

const Tool = () => {
  return (
    <DesignerApi messages={{}} defaultConfig={{ Configuration: { QVDFile: '' }}}>
      <AyxAppWrapper> 
        <App />
      </AyxAppWrapper>
    </DesignerApi>
  )
}

ReactDOM.render(
  <Tool />,
  document.getElementById('app')
);
